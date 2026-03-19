import httpx
import json
import sys

URL = "https://ows.goszakup.gov.kz/v3/graphql"
HEADERS = {
    "Authorization": "Bearer 9e3c82c2e1c542588ef7ae4484e073b4",
    "Content-Type": "application/json",
}
TIMEOUT = 60.0

def run_query(query: str, variables: dict | None = None) -> dict:
    payload = {"query": query}
    if variables:
        payload["variables"] = variables # type: ignore
    resp = httpx.post(URL, json=payload, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        print(f"[GraphQL errors] {json.dumps(data['errors'], indent=2, ensure_ascii=False)}", file=sys.stderr)
    return data


def type_ref_str(t: dict) -> str:
    """Recursively resolve a GraphQL type reference to a human-readable string."""
    if t is None:
        return "?"
    kind = t.get("kind", "")
    name = t.get("name")
    of = t.get("ofType")
    if kind == "NON_NULL":
        return f"{type_ref_str(of)}!" # type: ignore
    if kind == "LIST":
        return f"[{type_ref_str(of)}]" # type: ignore
    return name or "?"


INTROSPECTION_QUERY = """
{
  __schema {
    queryType { name }
    mutationType { name }
    subscriptionType { name }
    types {
      kind
      name
      description
      fields(includeDeprecated: true) {
        name
        description
        type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
        args {
          name
          type {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
      }
      inputFields {
        name
        type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
            }
          }
        }
      }
      enumValues(includeDeprecated: true) {
        name
        description
      }
    }
  }
}
"""

print("=" * 90)
print("  GOSZAKUP GraphQL API  —  FULL INTROSPECTION")
print("=" * 90)

data = run_query(INTROSPECTION_QUERY)
schema = data["data"]["__schema"]
all_types = schema["types"]

builtin_prefixes = ("__",)
scalar_builtins = {"String", "Int", "Float", "Boolean", "ID"}

user_types = [t for t in all_types if not t["name"].startswith("__")]
object_types = [t for t in user_types if t["kind"] == "OBJECT"]
input_types = [t for t in user_types if t["kind"] == "INPUT_OBJECT"]
enum_types = [t for t in user_types if t["kind"] == "ENUM"]
scalar_types = [t for t in user_types if t["kind"] == "SCALAR" and t["name"] not in scalar_builtins]

print(f"\nQuery root     : {schema['queryType']['name'] if schema.get('queryType') else 'none'}")
print(f"Mutation root  : {schema['mutationType']['name'] if schema.get('mutationType') else 'none'}")
print(f"Subscription   : {schema['subscriptionType']['name'] if schema.get('subscriptionType') else 'none'}")
print(f"\nTotal user types : {len(user_types)}")
print(f"  OBJECT         : {len(object_types)}")
print(f"  INPUT_OBJECT   : {len(input_types)}")
print(f"  ENUM           : {len(enum_types)}")
print(f"  SCALAR (custom): {len(scalar_types)}")

print("\n" + "=" * 90)
print("  ROOT QUERY FIELDS (available top-level queries)")
print("=" * 90)

query_type_name = schema["queryType"]["name"]
query_type = next(t for t in all_types if t["name"] == query_type_name)

for f in sorted(query_type["fields"], key=lambda x: x["name"]):
    args_str = ""
    if f["args"]:
        args_parts = [f'{a["name"]}: {type_ref_str(a["type"])}' for a in f["args"]]
        args_str = f'({", ".join(args_parts)})'
    print(f"  {f['name']}{args_str}  ->  {type_ref_str(f['type'])}")

MAIN_ENTITIES = [
    "TrdBuy", "Lots", "Contract", "Plans", "Subject",
    "TrdApp", "Payment", "ContractAct",
    "TrdBuyRow", "LotsRow", "ContractRow", "PlansRow", "SubjectRow",
    "TrdAppRow", "PaymentRow", "ContractActRow",
]

obj_by_name = {t["name"]: t for t in object_types}

print("\n" + "=" * 90)
print("  MAIN ENTITY TYPES  —  fields & relationships")
print("=" * 90)

printed_entities = set()
for ename in MAIN_ENTITIES:
    if ename in obj_by_name:
        printed_entities.add(ename)

for t in object_types:
    for kw in ["TrdBuy", "Lot", "Contract", "Plan", "Subject", "TrdApp", "Payment", "ContractAct",
               "Announce", "RefBuy", "Bid"]:
        if kw.lower() in t["name"].lower() and t["name"] not in printed_entities:
            printed_entities.add(t["name"])

for tname in sorted(printed_entities):
    t = obj_by_name[tname]
    fields = t.get("fields") or []
    print(f"\n{'─' * 80}")
    desc = f'  ({t["description"]})' if t.get("description") else ""
    print(f"  {tname}{desc}")
    print(f"{'─' * 80}")
    for f in sorted(fields, key=lambda x: x["name"]):
        ft = type_ref_str(f["type"])
        desc_f = f'  # {f["description"]}' if f.get("description") else ""
        print(f"    {f['name']:40s} : {ft}{desc_f}")

print("\n" + "=" * 90)
print("  REFERENCE / DICTIONARY TYPES  (names containing 'Ref')")
print("=" * 90)

ref_types = [t for t in object_types if "ref" in t["name"].lower() or "dict" in t["name"].lower()]
for t in sorted(ref_types, key=lambda x: x["name"]):
    fields = t.get("fields") or []
    field_strs = [f'{f["name"]}: {type_ref_str(f["type"])}' for f in fields]
    desc = f'  ({t["description"]})' if t.get("description") else ""
    print(f"\n  {t['name']}{desc}")
    print(f"    -> [{', '.join(field_strs)}]")

if enum_types:
    print("\n" + "=" * 90)
    print("  ENUM TYPES")
    print("=" * 90)
    for t in sorted(enum_types, key=lambda x: x["name"]):
        vals = [v["name"] for v in (t.get("enumValues") or [])]
        print(f"\n  {t['name']}: {', '.join(vals)}")

if input_types:
    print("\n" + "=" * 90)
    print("  INPUT TYPES  (used as query arguments)")
    print("=" * 90)
    for t in sorted(input_types, key=lambda x: x["name"]):
        ifields = t.get("inputFields") or []
        field_strs = [f'{f["name"]}: {type_ref_str(f["type"])}' for f in ifields]
        print(f"\n  {t['name']}")
        print(f"    -> [{', '.join(field_strs)}]")

remaining = [t for t in object_types
             if t["name"] not in printed_entities
             and t["name"] != query_type_name
             and not any(t["name"] == rt["name"] for rt in ref_types)]

if remaining:
    print("\n" + "=" * 90)
    print("  OTHER OBJECT TYPES")
    print("=" * 90)
    for t in sorted(remaining, key=lambda x: x["name"]):
        fields = t.get("fields") or []
        field_strs = [f'{f["name"]}: {type_ref_str(f["type"])}' for f in fields]
        desc = f'  ({t["description"]})' if t.get("description") else ""
        print(f"\n  {t['name']}{desc}")
        print(f"    -> [{', '.join(field_strs)}]")

print("\n" + "=" * 90)
print("  SUMMARY: All types at a glance")
print("=" * 90)
for t in sorted(user_types, key=lambda x: (x["kind"], x["name"])):
    fc = len(t.get("fields") or t.get("inputFields") or t.get("enumValues") or [])
    print(f"  [{t['kind']:12s}]  {t['name']:40s}  ({fc} fields/values)")

print("\nDone.")
