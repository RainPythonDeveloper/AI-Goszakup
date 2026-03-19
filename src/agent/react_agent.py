"""
ReAct AI Agent — агент для анализа госзакупок.

Паттерн ReAct: Reason → Act → Observe → Repeat.
LLM выбирает инструменты, получает результаты, формирует ответ.
LLM НЕ считает сама — делегирует SQL/Python через tools.

Улучшения:
- Structured tracing (AgentTrace) для дебага reasoning-цепочек
- Retry с exponential backoff для LLM-вызовов
- Async-совместимость DB через run_in_executor
- Context window management (sliding window)
- Приоритетный классификатор вопросов
- HTTP client reuse
- Rate limiting на tool calls per message
"""
import asyncio
import json
import logging
import re
import time
from collections import Counter
from dataclasses import dataclass, field

import httpx

from src.config import llm_config
from src.agent.tools import TOOL_DEFINITIONS, call_tool
from src.agent.prompts import build_system_prompt, get_question_hint

logger = logging.getLogger(__name__)

@dataclass
class ToolCallTrace:
    """Трейс одного вызова инструмента."""
    tool_name: str
    arguments: dict
    result_size: int
    duration_ms: int
    error: str | None = None


@dataclass
class IterationTrace:
    """Трейс одной итерации ReAct-цикла."""
    iteration: int
    thought: str | None = None  # извлечённое рассуждение из content
    tool_calls: list[ToolCallTrace] = field(default_factory=list)
    finish_reason: str = ""
    is_final: bool = False


@dataclass
class AgentTrace:
    """Полный трейс обработки вопроса."""
    question: str
    question_type: str
    iterations: list[IterationTrace] = field(default_factory=list)
    final_answer: str = ""
    total_time_ms: int = 0
    tools_used: list[str] = field(default_factory=list)
    total_tool_calls: int = 0
    error: str | None = None


# Приоритеты типов: более специфичные типы побеждают при конфликте
_TYPE_PRIORITY = {
    "справедливость_цены": 5,  # наивысший — самый специфичный
    "аномалии": 4,
    "сравнение": 3,
    "аналитика": 2,
    "поиск": 1,  # наименьший — самый общий
}

QUESTION_TYPES = {
    "поиск": [
        r"найди", r"покажи", r"выведи", r"список", r"какие", r"сколько",
        r"кто", r"где", r"search", r"find", r"табл",
        # Kazakh
        r"іздеу", r"көрсет", r"табу", r"тап\b", r"тізім", r"қандай",
        r"қанша", r"неше", r"кім", r"қайда",
    ],
    "сравнение": [
        r"сравни", r"сопоставь", r"vs", r"против", r"разница", r"отличи",
        r"больше|меньше.*чем", r"относительно",
        # Kazakh
        r"салыстыр", r"айырмашылық", r"көп.*қарағанда", r"аз.*қарағанда",
    ],
    "аналитика": [
        r"топ", r"top", r"рейтинг", r"тренд", r"динамик", r"статистик",
        r"средн", r"итого", r"агрегат", r"обзор",
        # Kazakh
        r"талдау", r"шолу", r"жиынтық", r"орташа",
    ],
    "аномалии": [
        r"аномал", r"выброс", r"подозрит", r"отклонен", r"завышен",
        r"занижен", r"необычн", r"anomal",
        r"нетипичн", r"отклонени.*цен", r"завышени.*цен", r"завышени.*количеств",
        r"концентрац.*поставщик",
        # Kazakh
        r"ауытқу", r"күдікті", r"жоғарылат", r"төмендет",
        r"әдеттен\s*тыс", r"жеткізуші.*шоғырлану",
    ],
    "справедливость_цены": [
        r"справедлив", r"адекватн", r"fair.?price", r"рыночн.*цен",
        r"обоснован.*цен", r"оценк.*цен",
        r"оцени.*цен.*лот", r"цена.*лота",
        # Kazakh
        r"әділ\s*баға", r"нарықтық\s*баға", r"бағ[аа].*бағала", r"лот.*бағасы",
    ],
}


_KK_SPECIFIC = set("ӘәҒғҚқҢңӨөҰұҮүІі")
_KK_WORDS = re.compile(
    r"\b(қандай|қанша|неше|кім|қайда|бойынша|көрсет|табу|тап|іздеу|салыстыр|"
    r"талдау|ауытқу|бағ[аа]|сатып\s?алу|келісімшарт|жеткізуші|тапсырысшы|"
    r"әділ|бағалау|лот|сома|кезең|аймақ|ұсыныс|мемлекеттік|жылда|нәтиже|"
    r"шарт|тендер|қамтамасыз|орташа|жиынтық|шолу|рейтинг|күдікті|"
    r"жоғарылат|төмендет|шоғырлану|нарықтық)\b", re.I
)


def detect_language(text: str) -> str:
    """
    Определяет язык сообщения: 'kk' (казахский) или 'ru' (русский).

    Стратегия:
    1. Если в тексте есть специфичные казахские буквы (ә, ғ, қ, ң, ө, ұ, ү, і) —
       и их доля > 2% от всех кириллических символов → казахский.
    2. Если специфичных букв нет — ищем казахские слова (regex).
       Если >= 2 совпадений → казахский.
    3. Иначе → русский.
    """
    cyrillic = sum(1 for c in text if '\u0400' <= c <= '\u04FF')
    kk_chars = sum(1 for c in text if c in _KK_SPECIFIC)

    if cyrillic > 0 and kk_chars / cyrillic > 0.02:
        return "kk"

    if len(_KK_WORDS.findall(text)) >= 2:
        return "kk"

    return "ru"


def classify_question(text: str) -> str:
    """
    Определяет тип вопроса по ключевым словам.

    При равном количестве совпадений побеждает тип с более высоким приоритетом
    (более специфичный). Это решает конфликт "покажи аномалии" — аномалии > поиск.
    """
    text_lower = text.lower()
    scores: Counter = Counter()

    for qtype, patterns in QUESTION_TYPES.items():
        for pattern in patterns:
            if re.search(pattern, text_lower):
                scores[qtype] += 1

    if not scores:
        return "поиск"

    max_score = scores.most_common(1)[0][1]
    top_types = [t for t, s in scores.items() if s == max_score]

    if len(top_types) == 1:
        return top_types[0]

    return max(top_types, key=lambda t: _TYPE_PRIORITY.get(t, 0))


def _try_parse_tool_calls_from_content(content: str) -> list[dict] | None:
    """
    Fallback: пытается извлечь tool calls из текстового content LLM.
    Некоторые модели могут возвращать JSON с вызовами инструментов в content
    вместо использования нативного tool_calls формата.
    """
    if not content:
        return None

    patterns = [
        r'\{[^{}]*"name"\s*:\s*"(\w+)"[^{}]*"arguments"\s*:\s*(\{[^{}]*\})[^{}]*\}',
        r'```json\s*(\{.*?\})\s*```',
    ]

    tool_calls = []
    for pattern in patterns:
        matches = re.finditer(pattern, content, re.DOTALL)
        for i, match in enumerate(matches):
            try:
                if match.lastindex == 2:
                    name = match.group(1)
                    args = json.loads(match.group(2))
                    tool_calls.append({
                        "id": f"fallback_{i}",
                        "function": {"name": name, "arguments": json.dumps(args)},
                    })
                else:
                    obj = json.loads(match.group(1))
                    if "name" in obj:
                        tool_calls.append({
                            "id": f"fallback_{i}",
                            "function": {
                                "name": obj["name"],
                                "arguments": json.dumps(obj.get("arguments", {})),
                            },
                        })
            except (json.JSONDecodeError, IndexError):
                continue

    return tool_calls if tool_calls else None


def _extract_thought(content: str | None) -> str | None:
    """Извлекает рассуждение (thought) из content LLM для трейсинга."""
    if not content:
        return None

    for tag in ("thought", "plan", "analysis"):
        match = re.search(rf"<{tag}>(.*?)</{tag}>", content, re.DOTALL)
        if match:
            return match.group(1).strip()

    if len(content) > 20:
        return content[:500].strip()
    return None


def _estimate_tokens(text: str) -> int:
    """Приблизительная оценка количества токенов (1 токен ~ 4 символа для русского)."""
    return len(text) // 3


class ReActAgent:
    """ReAct агент с structured tracing, retry, async DB и context management."""

    MAX_CONTEXT_TOKENS = 30000
    MAX_HISTORY_PAIRS = 15
    MAX_TOOL_CALLS_PER_MESSAGE = 20

    def __init__(self):
        self.model = llm_config.model
        self.api_url = f"{llm_config.base_url.rstrip('/')}/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {llm_config.api_key}",
            "Content-Type": "application/json",
        }
        self.max_iterations = 8
        self.conversation_history: list[dict] = []
        self._http_client: httpx.AsyncClient | None = None
        self._last_trace: AgentTrace | None = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Переиспользуем HTTP client (connection pooling)."""
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=120.0)
        return self._http_client

    async def close(self):
        """Закрывает HTTP клиент."""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
            self._http_client = None

    def _build_tools_for_api(self) -> list[dict]:
        """Формирует tools в формате OpenAI API."""
        return [
            {
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool["description"],
                    "parameters": tool["parameters"],
                },
            }
            for tool in TOOL_DEFINITIONS
        ]

    async def _call_llm(self, messages: list[dict],
                         tools: list[dict] | None = None,
                         max_retries: int = 3) -> dict:
        """
        Вызывает LLM API с retry и exponential backoff.

        Ретраит на:
        - 429 (rate limit)
        - 500, 502, 503, 504 (server errors)
        - Timeout
        Fallback: если 400 с tools — пробуем без tools (модель может не поддерживать tool calling).
        """
        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": llm_config.max_tokens,
            "temperature": llm_config.temperature,
        }
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"

        client = await self._get_http_client()
        last_error = None

        for attempt in range(max_retries):
            try:
                resp = await client.post(self.api_url, json=payload, headers=self.headers)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as e:
                last_error = e
                status = e.response.status_code
                resp_text = e.response.text[:500] if e.response else "no body"
                logger.warning(f"LLM API error {status}: {resp_text}")

                if status == 400 and "tools" in payload:
                    logger.warning("LLM returned 400 with tools — retrying without tools (fallback)")
                    payload.pop("tools", None)
                    payload.pop("tool_choice", None)
                    continue

                if status == 429 or status >= 500:
                    wait = 2 ** attempt
                    logger.warning(
                        f"LLM API error {status}, retry {attempt + 1}/{max_retries} in {wait}s"
                    )
                    await asyncio.sleep(wait)
                    continue
                raise
            except httpx.TimeoutException as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    logger.warning(f"LLM API timeout, retry {attempt + 1}/{max_retries} in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                raise

        raise last_error  # type: ignore[misc]

    def _trim_history(self):
        """
        Sliding window: обрезает историю до MAX_HISTORY_PAIRS пар user/assistant.
        Сохраняет последние N пар сообщений.
        """
        if len(self.conversation_history) <= self.MAX_HISTORY_PAIRS * 2:
            return

        self.conversation_history = self.conversation_history[-(self.MAX_HISTORY_PAIRS * 2):]
        logger.info(f"History trimmed to {len(self.conversation_history)} messages")

    def _truncate_tool_result(self, result: dict, func_name: str) -> str:
        """Умное обрезание результата инструмента."""
        result_str = json.dumps(result, ensure_ascii=False, default=str, separators=(',', ':'))

        if len(result_str) <= 8000:
            return result_str

        if isinstance(result, dict):
            for key in ("anomalies", "rows", "alerts", "items"):
                if key in result and isinstance(result[key], list) and len(result[key]) > 15:
                    total = len(result[key])
                    result[key] = result[key][:15]
                    result[f"{key}_truncated"] = True
                    result[f"{key}_total_count"] = total
                    result[f"{key}_note"] = f"Показано 15 из {total}. Учитывай полное количество ({total}) в ответе."
            result_str = json.dumps(result, ensure_ascii=False, default=str, separators=(',', ':'))

        if len(result_str) > 12000:
            result_str = result_str[:12000] + '..."}'
            logger.warning(f"Tool {func_name} output truncated to 12000 chars")

        return result_str

    async def chat(self, user_message: str) -> str:
        """
        Обрабатывает сообщение пользователя через ReAct цикл.

        Returns:
            Текстовый ответ агента.
        """
        start_time = time.monotonic()

        question_type = classify_question(user_message)
        lang = detect_language(user_message)
        logger.info(f"Question type: {question_type} | Lang: {lang} | Message: {user_message[:100]}")
        trace = AgentTrace(question=user_message, question_type=question_type)

        hint = get_question_hint(question_type)
        augmented_message = f"{hint}\n\n{user_message}" if hint else user_message

        self.conversation_history.append({
            "role": "user",
            "content": user_message,
        })
        self._trim_history()

        system_prompt = build_system_prompt(lang=lang)

        messages = [
            {"role": "system", "content": system_prompt},
            *self.conversation_history[:-1],
            {"role": "user", "content": augmented_message},
        ]

        tools = self._build_tools_for_api()
        total_tool_calls = 0
        _repeated_error_calls = 0

        for iteration in range(self.max_iterations):
            logger.info(f"ReAct iteration {iteration + 1}/{self.max_iterations}")
            iter_trace = IterationTrace(iteration=iteration + 1)

            try:
                response = await self._call_llm(messages, tools)
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                resp_body = e.response.text[:500] if e.response else ""
                error_msg = f"LLM API HTTP error: {status}"
                logger.error(f"{error_msg} - {resp_body}")
                trace.error = error_msg

                if iteration == 0 and tools:
                    logger.warning("Attempting final fallback: call LLM without tools")
                    try:
                        response = await self._call_llm(messages, tools=None)
                    except Exception as fallback_err:
                        logger.error(f"Fallback also failed: {fallback_err}")
                        self._finalize_trace(trace, start_time, "Ошибка при обращении к LLM. Попробуйте переформулировать вопрос.")
                        return trace.final_answer
                else:
                    self._finalize_trace(trace, start_time, "Ошибка при обращении к LLM. Попробуйте переформулировать вопрос.")
                    return trace.final_answer
            except httpx.TimeoutException:
                logger.error("LLM API timeout after all retries")
                trace.error = "LLM timeout"
                self._finalize_trace(trace, start_time, "Превышено время ожидания ответа от LLM. Попробуйте повторить запрос.")
                return trace.final_answer
            except Exception as e:
                logger.error(f"LLM API unexpected error: {e}")
                trace.error = str(e)
                self._finalize_trace(trace, start_time, "Внутренняя ошибка. Попробуйте позже.")
                return trace.final_answer

            choice = response["choices"][0]
            message = choice["message"]
            finish_reason = choice.get("finish_reason", "unknown")
            iter_trace.finish_reason = finish_reason

            content = message.get("content") or ""
            iter_trace.thought = _extract_thought(content)

            has_tool_calls = bool(message.get("tool_calls"))
            logger.info(
                f"LLM response: finish_reason={finish_reason}, "
                f"has_tool_calls={has_tool_calls}, "
                f"content_preview='{content[:200]}'"
            )

            tool_calls = message.get("tool_calls")

            if not tool_calls and content:
                parsed = _try_parse_tool_calls_from_content(content)
                if parsed:
                    logger.info(f"Fallback: parsed {len(parsed)} tool calls from content")
                    tool_calls = parsed

            if not tool_calls:
                final_answer = content

                _is_plan_not_answer = (
                    total_tool_calls == 0
                    and final_answer.strip().startswith("{")
                    and iteration < self.max_iterations - 1
                )

                if (not final_answer.strip() or _is_plan_not_answer) and iteration < self.max_iterations - 1:
                    if _is_plan_not_answer:
                        logger.warning(f"LLM returned plan/JSON instead of answer on iteration {iteration}, forcing tool use")
                        retry_msg = (
                            "Ты вернул свой план вместо реального ответа. "
                            "ВЫЗОВИ инструменты для получения данных. "
                            f"Вопрос пользователя: {user_message}"
                        )
                    else:
                        logger.warning(f"Empty response on iteration {iteration}, retrying with stronger prompt")
                        retry_msg = (
                            "Ты ОБЯЗАН ответить на вопрос пользователя. "
                            "У тебя уже есть данные от инструментов выше. "
                            "Сформируй развёрнутый ответ на основе полученных данных. "
                            "НЕ вызывай инструменты — просто ответь текстом."
                        )
                    messages.append({"role": "user", "content": retry_msg})
                    trace.iterations.append(iter_trace)
                    continue

                if not final_answer.strip():
                    logger.warning("Final answer is empty after retries, using fallback")
                    final_answer = (
                        "Не удалось сформировать ответ на ваш вопрос. "
                        "Попробуйте переформулировать или задать более конкретный вопрос."
                    )

                iter_trace.is_final = True
                trace.iterations.append(iter_trace)

                self.conversation_history.append({
                    "role": "assistant",
                    "content": final_answer,
                })
                self._finalize_trace(trace, start_time, final_answer)
                return final_answer

            messages.append(message)

            if total_tool_calls + len(tool_calls) > self.MAX_TOOL_CALLS_PER_MESSAGE:
                logger.warning(
                    f"Tool call limit reached ({total_tool_calls}/{self.MAX_TOOL_CALLS_PER_MESSAGE}), "
                    f"requesting final answer"
                )
                break

            for tool_call in tool_calls:
                func_name = tool_call["function"]["name"]
                try:
                    func_args = json.loads(tool_call["function"]["arguments"])
                except json.JSONDecodeError:
                    func_args = {}

                logger.info(f"Calling tool: {func_name}({json.dumps(func_args, ensure_ascii=False)[:200]})")

                tool_start = time.monotonic()
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, call_tool, func_name, func_args)
                tool_duration_ms = int((time.monotonic() - tool_start) * 1000)

                result_str = self._truncate_tool_result(result, func_name)
                total_tool_calls += 1

                tool_trace = ToolCallTrace(
                    tool_name=func_name,
                    arguments=func_args,
                    result_size=len(result_str),
                    duration_ms=tool_duration_ms,
                    error=result.get("error") if isinstance(result, dict) else None,
                )
                iter_trace.tool_calls.append(tool_trace)

                if func_name not in trace.tools_used:
                    trace.tools_used.append(func_name)

                logger.info(f"Tool {func_name} returned {len(result_str)} chars in {tool_duration_ms}ms")

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "content": result_str,
                })

            trace.iterations.append(iter_trace)

            all_errors = all(
                tc.error for tc in iter_trace.tool_calls
            ) if iter_trace.tool_calls else False

            if all_errors:
                _repeated_error_calls += 1
                if _repeated_error_calls >= 2:
                    logger.warning(
                        f"Detected tool call loop ({_repeated_error_calls} consecutive error iterations), "
                        f"forcing final answer"
                    )
                    tools = None
                    messages.append({
                        "role": "user",
                        "content": (
                            "СТОП. Ты зациклился на ошибочных вызовах инструментов. "
                            "У тебя УЖЕ ЕСТЬ данные от предыдущих успешных вызовов выше. "
                            "Сформируй ТЕКСТОВЫЙ ответ пользователю на основе этих данных. "
                            "НЕ вызывай инструменты."
                        ),
                    })
                    continue
            else:
                _repeated_error_calls = 0

        logger.warning(f"Max iterations ({self.max_iterations}) reached, requesting final answer")
        try:
            final_response = await self._call_llm(messages)
            answer = final_response["choices"][0]["message"].get("content", "") or ""
            if not answer.strip():
                answer = "Не удалось получить финальный ответ. Попробуйте упростить вопрос."
        except Exception as e:
            logger.error(f"Final answer LLM call failed: {e}")
            answer = "Не удалось получить финальный ответ. Попробуйте упростить вопрос."

        self.conversation_history.append({
            "role": "assistant",
            "content": answer,
        })
        self._finalize_trace(trace, start_time, answer)
        return answer

    def _finalize_trace(self, trace: AgentTrace, start_time: float, answer: str):
        """Завершает трейс и сохраняет."""
        trace.final_answer = answer
        trace.total_time_ms = int((time.monotonic() - start_time) * 1000)
        trace.total_tool_calls = sum(
            len(it.tool_calls) for it in trace.iterations
        )
        self._last_trace = trace

        logger.info(
            f"AgentTrace: type={trace.question_type}, "
            f"iterations={len(trace.iterations)}, "
            f"tools={trace.tools_used}, "
            f"tool_calls={trace.total_tool_calls}, "
            f"time={trace.total_time_ms}ms, "
            f"error={trace.error}"
        )

    @property
    def last_trace(self) -> AgentTrace | None:
        """Возвращает трейс последнего вызова для дебага и метрик."""
        return self._last_trace

    def get_metrics(self) -> dict | None:
        """Возвращает метрики последнего вызова."""
        if not self._last_trace:
            return None
        t = self._last_trace
        return {
            "question_type": t.question_type,
            "response_time_ms": t.total_time_ms,
            "iterations_used": len(t.iterations),
            "tools_used": t.tools_used,
            "total_tool_calls": t.total_tool_calls,
            "error": t.error,
        }

    def reset(self):
        """Сбрасывает историю диалога."""
        self.conversation_history = []
        self._last_trace = None


_agent_instance: ReActAgent | None = None


def get_agent() -> ReActAgent:
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = ReActAgent()
    return _agent_instance
