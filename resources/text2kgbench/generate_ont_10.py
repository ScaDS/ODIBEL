import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Award",
        "dbo:City",
        "dbo:Country",
        "dbo:Place",
        "dbo:Series",
        "dbo:Voice",
        "dbo:Person",
        "dbo:Organisation",
        "dbo:Film",
        "dbo:ComicsCharacter"
    ]

    properties = [
        "dbo:alternativeName",
        "dbo:award",
        "dbo:birthPlace",
        "dbo:broadcastedBy",
        "dbo:child",
        "dbo:city",
        "dbo:creator",
        "dbo:distributor",
        "dbo:firstAired",
        "dbo:firstAppearanceInFilm",
        "dbo:foundedBy",
        "dbo:fullName",
        "dbo:keyPerson",
        "dbo:lastAired",
        "dbo:nationality",
        "dbo:series",
        "dbo:starring",
        "dbo:voice"
    ]

    generate(classes, properties, "ont_10", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])