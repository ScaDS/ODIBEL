import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:CityType",
        "dbo:County",
        "dbo:Demonym",
        "dbo:EthnicGroup",
        "dbo:GovernmentType",
        "dbo:Language",
        "dbo:Place",
        "dbo:State",
        "dbo:Timezone",
        "dbo:Country",
        "dbo:City"
    ]

    properties = [
        "dbo:areaCode",
        "dbo:areaOfLand",
        "dbo:areaTotal",
        "dbo:capital",
        "dbo:country",
        "dbo:countySeat",
        "dbo:demonym",
        "dbo:elevationAboveTheSeaLevel",
        "dbo:ethnicGroup",
        "dbo:governmentType",
        "dbo:isPartOf",
        "dbo:language",
        "dbo:largestCity",
        "dbo:leader",
        "dbo:leaderTitle",
        "dbo:location",
        "dbo:populationDensity",
        "dbo:populationMetro",
        "dbo:postalCode",
        "dbo:state",
        "dbo:timeZone",
        "dbo:type",
        "dbo:utcOffset"
    ]

    generate(classes, properties, "ont_16", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])