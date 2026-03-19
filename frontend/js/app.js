const clientId = 'user_' + Math.random().toString(36).substr(2, 9);
const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
const chat = document.getElementById('chat');
const input = document.getElementById('input');
const btn = document.getElementById('btn');

let ws;
let wsReady = false;
let sendTimeout = null;

const L = {
    ru: {
        title: 'AI Госзакупки РК',
        subtitle: 'Аналитика государственных закупок',
        placeholder: 'Задайте вопрос о госзакупках...',
        send: 'Отправить',
        examples: 'Примеры запросов:',
        noConnection: 'Нет соединения с сервером. Подождите или обновите страницу.',
        connectionLost: 'Соединение потеряно. Переподключение...',
        emptyResponse: 'Пустой ответ от сервера. Попробуйте ещё раз.',
        sendError: 'Ошибка отправки сообщения. Обновите страницу.',
        timeout: 'Сервер не ответил вовремя. Попробуйте ещё раз.',
        toggleLabel: 'ҚАЗ',
    },
    kk: {
        title: 'AI Мемлекеттік сатып алу РҚ',
        subtitle: 'Мемлекеттік сатып алуды талдау',
        placeholder: 'Мемлекеттік сатып алу туралы сұрақ қойыңыз...',
        send: 'Жіберу',
        examples: 'Сұрау мысалдары:',
        noConnection: 'Сервермен байланыс жоқ. Күтіңіз немесе бетті жаңартыңыз.',
        connectionLost: 'Байланыс үзілді. Қайта қосылу...',
        emptyResponse: 'Серверден бос жауап. Қайтадан көріңіз.',
        sendError: 'Хабарлама жіберу қатесі. Бетті жаңартыңыз.',
        timeout: 'Сервер уақытында жауап бермеді. Қайтадан көріңіз.',
        toggleLabel: 'РУС',
    },
};

let currentLang = localStorage.getItem('lang') || 'ru';

function toggleLang() {
    currentLang = currentLang === 'ru' ? 'kk' : 'ru';
    localStorage.setItem('lang', currentLang);
    applyLang();
}

function applyLang() {
    const t = L[currentLang];
    const headerLeft = document.querySelector('.header-left');
    if (headerLeft) {
        headerLeft.childNodes[0].textContent = t.title + '\n';
    }
    const subtitle = document.getElementById('header-subtitle');
    if (subtitle) subtitle.textContent = t.subtitle;

    const toggle = document.getElementById('lang-toggle');
    if (toggle) toggle.textContent = t.toggleLabel;

    input.placeholder = t.placeholder;
    btn.textContent = t.send;

    const exTitle = document.getElementById('examples-title');
    if (exTitle) exTitle.textContent = t.examples;

    // Update example buttons text
    document.querySelectorAll('#examples button').forEach(b => {
        const text = b.getAttribute('data-' + currentLang);
        if (text) b.textContent = text;
    });

    document.documentElement.lang = currentLang === 'kk' ? 'kk' : 'ru';
}

applyLang();

function connectWs() {
    ws = new WebSocket(`${proto}//${location.host}/ws/chat/${clientId}`);

    ws.onopen = () => {
        wsReady = true;
        btn.disabled = false;
        input.disabled = false;
        const t = L[currentLang];
        document.querySelectorAll('.error').forEach(el => {
            if (el.textContent.includes(L.ru.connectionLost) ||
                el.textContent.includes(L.kk.connectionLost) ||
                el.textContent.includes('Переподключение') ||
                el.textContent.includes('Қайта қосылу')) {
                el.remove();
            }
        });
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        document.querySelectorAll('.thinking').forEach(el => el.remove());

        const div = document.createElement('div');
        div.className = 'message ' + (data.type === 'error' ? 'error' :
            data.type === 'thinking' ? 'thinking' : 'assistant');

        if (data.type === 'response') {
            const t = L[currentLang];
            const content = data.content || t.emptyResponse;
            div.innerHTML = marked.parse(content);
        } else {
            div.textContent = data.content;
        }

        chat.appendChild(div);
        chat.scrollTop = chat.scrollHeight;

        if (data.type !== 'thinking') {
            btn.disabled = false;
            input.disabled = false;
            if (sendTimeout) {
                clearTimeout(sendTimeout);
                sendTimeout = null;
            }
        }
    };

    ws.onerror = (err) => {
        console.error('WebSocket error:', err);
        wsReady = false;
    };

    ws.onclose = () => {
        wsReady = false;
        btn.disabled = false;
        input.disabled = false;
        document.querySelectorAll('.thinking').forEach(el => el.remove());
        if (sendTimeout) {
            clearTimeout(sendTimeout);
            sendTimeout = null;
        }
        addMessage(L[currentLang].connectionLost, 'error');
        setTimeout(connectWs, 3000);
    };
}

connectWs();

function send() {
    const text = input.value.trim();
    if (!text) return;
    const t = L[currentLang];

    if (!wsReady || ws.readyState !== WebSocket.OPEN) {
        addMessage(t.noConnection, 'error');
        return;
    }

    document.querySelector('.examples')?.remove();
    addMessage(text, 'user');

    try {
        ws.send(JSON.stringify({ message: text }));
    } catch (e) {
        console.error('ws.send failed:', e);
        addMessage(t.sendError, 'error');
        btn.disabled = false;
        input.disabled = false;
        return;
    }

    input.value = '';
    btn.disabled = true;
    input.disabled = true;

    sendTimeout = setTimeout(() => {
        if (btn.disabled) {
            document.querySelectorAll('.thinking').forEach(el => el.remove());
            addMessage(L[currentLang].timeout, 'error');
            btn.disabled = false;
            input.disabled = false;
        }
    }, 330000);
}

function sendExample(el) {
    input.value = el.textContent;
    send();
}

function addMessage(text, type) {
    const div = document.createElement('div');
    div.className = 'message ' + type;
    div.textContent = text;
    chat.appendChild(div);
    chat.scrollTop = chat.scrollHeight;
}
