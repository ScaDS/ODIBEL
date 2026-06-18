import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Category",
        "dbo:Currency",
        "dbo:EthnicGroup",
        "dbo:Language",
        "dbo:Material",
        "dbo:Municipality",
        "dbo:Organisation",
        "dbo:Religion",
        "dbo:State",
        "dbo:City",
        "dbo:Person",
        "dbo:Place",
        "dbo:Country",
        "dbo:Monument"
    ]

    properties = [
        "dbo:capital",
        "dbo:category",
        "dbo:country",
        "dbo:currency",
        "dbo:dedicatedTo",
        "dbo:designer",
        "dbo:district",
        "dbo:established",
        "dbo:ethnicGroup",
        "dbo:hasToItsNorth",
        "dbo:hasToItsSoutheast",
        "dbo:hasToItsSouthwest",
        "dbo:hasToItsWest",
        "dbo:inaugurationDate",
        "dbo:language",
        "dbo:largestCity",
        "dbo:leader",
        "dbo:leaderTitle",
        "dbo:location",
        "dbo:material",
        "dbo:municipality",
        "dbo:nativeName",
        "dbo:nearestCity",
        "dbo:owningOrganisation",
        "dbo:religion",
        "dbo:state"
    ]

    generate(classes, properties, "ont_12", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])