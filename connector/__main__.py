from __future__ import annotations

import argparse
import sys

import uvicorn

from connector.config import resolve_config
from connector.logging import configure_logging
from connector.register import RegistrationError, register_if_needed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m connector")
    subparsers = parser.add_subparsers(dest="command")

    register_parser = subparsers.add_parser(
        "register",
        help="Perform one-time connector registration if needed.",
    )
    register_parser.add_argument(
        "--online-url",
        dest="online_url",
        default=None,
        help="Override ONLINE_URL.",
    )
    register_parser.add_argument(
        "--enroll-token",
        dest="enroll_token",
        default=None,
        help="Override ENROLL_TOKEN.",
    )
    register_parser.add_argument(
        "--identity-path",
        dest="identity_path",
        default=None,
        help="Override CONNECTOR_IDENTITY_PATH.",
    )
    register_parser.add_argument(
        "--force",
        action="store_true",
        help="Delete existing identity and register again.",
    )

    serve_parser = subparsers.add_parser(
        "serve",
        help="Run the LocalConnector service mode.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "register":
        try:
            config = resolve_config(
                online_url_override=args.online_url,
                enroll_token_override=args.enroll_token,
                identity_path_override=args.identity_path,
            )
            identity = register_if_needed(
                config,
                force=args.force,
                enroll_token_override=args.enroll_token,
            )
        except (RegistrationError, ValueError, OSError) as exc:
            print(f"Registration failed: {exc}", file=sys.stderr)
            return 1

        print(f"Registration complete for device_id={identity['device_id']}")
        print(f"Identity stored at: {config.identity_path}")
        return 0

    elif args.command == "serve":
        config = resolve_config()
        # Ensure we pass a factory function, uvicorn imports and calls it
        uvicorn.run(
            "connector.main:create_app", 
            host=config.service_host, 
            port=config.service_port, 
            factory=True
        )
        return 0

    else:
        parser.print_help()
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
