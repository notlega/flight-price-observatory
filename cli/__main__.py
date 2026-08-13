"""CLI entry point dispatching to convert and search subcommands."""

import argparse


def main():
    parser = argparse.ArgumentParser(
        description="Flight Price Observatory CLI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    from cli.convert import configure_parser as configure_convert
    from cli.search import configure_parser as configure_search

    configure_search(subparsers)
    configure_convert(subparsers)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
