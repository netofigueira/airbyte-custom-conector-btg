#!/usr/bin/env python3

"""
Main entrypoint for the BTG Pactual Source connector.
This file serves as the entry point for Airbyte to run the connector.
"""

import sys
import traceback
from airbyte_cdk.entrypoint import launch
from source_btg import SourceBtg


def main():
    """Main function to launch the BTG Source connector."""
    try:
        source = SourceBtg()
        launch(source, sys.argv[1:])
    except Exception as e:
        print(f"Fatal error in BTG Source: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()