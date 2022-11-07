import json

from src.data_processor import get_metadata_index


def main():
    with open('index.json', 'w') as file_handler:
        json.dump(get_metadata_index(), file_handler, indent=2)



if __name__ == "__main__":
    main()
