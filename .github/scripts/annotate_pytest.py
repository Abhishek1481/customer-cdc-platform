"""Turn pytest JUnit XML failures into GitHub Actions error annotations."""

import sys
import xml.etree.ElementTree as ET


def escape(text: str) -> str:
    return text.replace("%", "%25").replace("\r", "").replace("\n", "%0A")


def main(path: str) -> None:
    for case in ET.parse(path).iter("testcase"):
        for bad in [*case.iter("failure"), *case.iter("error")]:
            message = f"{bad.get('message') or ''} | {(bad.text or '')[-1500:]}"
            title = f"{case.get('classname')}.{case.get('name')}"
            print(f"::error title={title}::{escape(message)}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "pytest-results.xml")
