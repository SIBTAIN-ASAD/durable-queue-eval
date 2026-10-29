"""Starting point: implement the durable queue described in the task."""
import argparse
import json
import sys


def handle(db_path, request):
    raise NotImplementedError("Queue operations are not implemented")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    args = parser.parse_args()
    for line in sys.stdin:
        try:
            request = json.loads(line)
            result = handle(args.db, request)
            response = {"ok": True, "result": result}
        except (ValueError, TypeError, KeyError):
            response = {"ok": False, "error": "invalid"}
        except NotImplementedError:
            response = {"ok": False, "error": "unimplemented"}
        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
