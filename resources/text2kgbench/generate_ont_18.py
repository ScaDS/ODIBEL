import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Award",
        "dbo:Currency",
        "dbo:Demonym",
        "dbo:Field",
        "dbo:School",
        "dbo:Timezone",
        "dbo:Type",
        "dbo:WrittenWork",
        "dbo:Language",
        "dbo:Organisation",
        "dbo:University",
        "dbo:Place",
        "dbo:City",
        "dbo:Country",
        "dbo:Scientist"
    ]

    properties = [
        "dbo:affiliation",
        "dbo:almaMater",
        "dbo:areaCode",
        "dbo:areaMetro",
        "dbo:areaOfWater",
        "dbo:areaTotal",
        "dbo:award",
        "dbo:birthDate",
        "dbo:birthName",
        "dbo:birthPlace",
        "dbo:capital",
        "dbo:chancellor",
        "dbo:citizenship",
        "dbo:country",
        "dbo:currency",
        "dbo:deathDate",
        "dbo:deathPlace",
        "dbo:demonym",
        "dbo:dissolutionYear",
        "dbo:doctoralAdvisor",
        "dbo:foundingDate",
        "dbo:foundingYear",
        "dbo:governmentType",
        "dbo:gridReference",
        "dbo:influencedBy",
        "dbo:isPartOf",
        "dbo:knownFor",
        "dbo:language",
        "dbo:leader",
        "dbo:leaderTitle",
        "dbo:longName",
        "dbo:motto",
        "dbo:nationality",
        "dbo:officialLanguage",
        "dbo:percentageOfAreaWater",
        "dbo:populationMetroDensity",
        "dbo:populationTotal",
        "dbo:postalCode",
        "dbo:professionalField",
        "dbo:region",
        "dbo:religion",
        "dbo:residence",
        "dbo:spouse",
        "dbo:timeZone",
        "dbo:type",
        "dbo:utcOffset",
        "dbo:viceChancellor"
    ]

    generate(classes, properties, "ont_18", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])