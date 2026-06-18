import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:City",
        "dbo:Currency",
        "dbo:Demonym",
        "dbo:Division",
        "dbo:EthnicGroup",
        "dbo:FoodFamily",
        "dbo:Genus",
        "dbo:Language",
        "dbo:Place",
        "dbo:Person",
        "dbo:Country",
        "dbo:Food"
    ]

    properties = [
        "dbo:alternativeName",
        "dbo:capital",
        "dbo:carbohydrate",
        "dbo:country",
        "dbo:course",
        "dbo:creator",
        "dbo:currency",
        "dbo:demonym",
        "dbo:dishVariation",
        "dbo:division",
        "dbo:ethnicGroup",
        "dbo:family",
        "dbo:fat",
        "dbo:genus",
        "dbo:ingredient",
        "dbo:isPartOf",
        "dbo:language",
        "dbo:leader",
        "dbo:leaderTitle",
        "dbo:mainIngredient",
        "dbo:order",
        "dbo:protein",
        "dbo:region",
        "dbo:servingTemperature"
    ]

    generate(classes, properties, "ont_13", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])