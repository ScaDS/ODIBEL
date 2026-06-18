import sys

from generate import generate

def run(input_path, output_path, ont_path):
    classes = [
        "dbo:Bird",
        "dbo:Commander",
        "dbo:Fossil",
        "dbo:Gemstone",
        "dbo:Mascot",
        "dbo:Organisation",
        "dbo:PartsType",
        "dbo:School",
        "dbo:State",
        "dbo:Country",
        "dbo:Place",
        "dbo:Award",
        "dbo:Person",
        "dbo:Mission",
        "dbo:Astronaut"
    ]

    properties = [
        "dbo:affiliation",
        "dbo:almaMater",
        "dbo:alternativeName",
        "dbo:award",
        "dbo:awards",
        "dbo:backupPilot",
        "dbo:bird",
        "dbo:birthDate",
        "dbo:birthPlace",
        "dbo:commander",
        "dbo:competeIn",
        "dbo:cosparId",
        "dbo:crewMembers",
        "dbo:dateOfRetirement",
        "dbo:deathDate",
        "dbo:deathPlace",
        "dbo:fossil",
        "dbo:gemstone",
        "dbo:higher",
        "dbo:isPartOf",
        "dbo:leader",
        "dbo:mascot",
        "dbo:mission",
        "dbo:nationality",
        "dbo:occupation",
        "dbo:operator",
        "dbo:part",
        "dbo:partsType",
        "dbo:president",
        "dbo:representative",
        "dbo:ribbonAward",
        "dbo:selectedByNasa",
        "dbo:senators",
        "dbo:servedAsChiefOfTheAstronautOfficeIn",
        "dbo:status",
        "dbo:timeInSpace",
        "dbo:title",
        "dbo:utcOffset"
    ]

    generate(classes, properties, "ont_9", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])