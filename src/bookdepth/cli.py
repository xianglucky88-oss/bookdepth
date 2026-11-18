
import json, sys
from . import pipeline


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    mode = "full"
    if "--incremental" in argv:
        mode = "incremental"
    if "--full" in argv:
        mode = "full"

    if "--book" in argv:
        kwargs = {}
        for i, token in enumerate(argv):
            if token == "--fail-after" and i + 1 < len(argv):
                kwargs["fail_after"] = int(argv[i + 1])
            if token.startswith("--fail-after="):
                kwargs["fail_after"] = int(token.split("=", 1)[1])
        if "--resume" in argv:
            kwargs["resume"] = True
        print(json.dumps(pipeline.run_book(mode=mode, **kwargs),
                         ensure_ascii=False))
        return

    print(json.dumps(pipeline.run(mode=mode), ensure_ascii=False))


if __name__ == "__main__":
    main()
