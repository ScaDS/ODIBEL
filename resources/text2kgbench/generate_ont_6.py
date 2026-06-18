import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Award",
        "dbo:City",
        "dbo:Commander",
        "dbo:Currency",
        "dbo:EthnicGroup",
        "dbo:Language",
        "dbo:MilitaryConflict",
        "dbo:Monarch",
        "dbo:Office",
        "dbo:Profession",
        "dbo:Religion",
        "dbo:School",
        "dbo:State",
        "dbo:Battle",
        "dbo:Party",
        "dbo:Year",
        "dbo:Organisation",
        "dbo:Country",
        "dbo:Politician"
    ]

    properties = [
        "dbo:activeYearsEndDate",
        "dbo:activeYearsStartDate",
        "dbo:affiliation",
        "dbo:almaMater",
        "dbo:award",
        "dbo:battle",
        "dbo:birthDate",
        "dbo:birthPlace",
        "dbo:birthYear",
        "dbo:commander",
        "dbo:country",
        "dbo:currency",
        "dbo:deathDate",
        "dbo:deathPlace",
        "dbo:deathYear",
        "dbo:ethnicGroup",
        "dbo:governingBody",
        "dbo:hasDeputy",
        "dbo:inOfficeWhileGovernor",
        "dbo:inOfficeWhileMonarch",
        "dbo:inOfficeWhilePresident",
        "dbo:inOfficeWhilePrimeMinister",
        "dbo:inOfficeWhileVicePresident",
        "dbo:isPartOfMilitaryConflict",
        "dbo:language",
        "dbo:largestCity",
        "dbo:leader",
        "dbo:militaryBranch",
        "dbo:nationality",
        "dbo:office",
        "dbo:party",
        "dbo:place",
        "dbo:predecessor",
        "dbo:profession",
        "dbo:region",
        "dbo:religion",
        "dbo:residence",
        "dbo:spouse",
        "dbo:state",
        "dbo:successor"
    ]

    generate(classes, properties, "ont_6", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])