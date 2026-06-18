import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Anthem",
        "dbo:Campus",
        "dbo:Colour",
        "dbo:EthnicGroup",
        "dbo:Religion",
        "dbo:River",
        "dbo:Saint",
        "dbo:Sport",
        "dbo:State",
        "dbo:Country",
        "dbo:Organization",
        "dbo:Place",
        "dbo:City",
        "dbo:Person",
        "dbo:University",
    ]

    properties = [
        "dbo:academicStaffSize",
        "dbo:affiliation",
        "dbo:anthem",
        "dbo:campus",
        "dbo:capital",
        "dbo:city",
        "dbo:country",
        "dbo:dean",
        "dbo:director",
        "dbo:elevationAboveTheSeaLevel",
        "dbo:established",
        "dbo:ethnicGroup",
        "dbo:founder",
        "dbo:governmentType",
        "dbo:hasToItsNortheast",
        "dbo:hasToItsNorthwest",
        "dbo:hasToItsWest",
        "dbo:headquarter",
        "dbo:isPartOf",
        "dbo:largestCity",
        "dbo:latinName",
        "dbo:leader",
        "dbo:leaderTitle",
        "dbo:legislature",
        "dbo:location",
        "dbo:longName",
        "dbo:motto",
        "dbo:neighboringMunicipality",
        "dbo:nickname",
        "dbo:numberOfDoctoralStudents",
        "dbo:numberOfPostgraduateStudents",
        "dbo:numberOfStudents",
        "dbo:numberOfUndergraduateStudents",
        "dbo:officialSchoolColour",
        "dbo:outlookRanking",
        "dbo:patronSaint",
        "dbo:postalCode",
        "dbo:president",
        "dbo:rector",
        "dbo:religion",
        "dbo:river",
        "dbo:sportGoverningBody",
        "dbo:sportsOffered",
        "dbo:staff",
        "dbo:state",
        "dbo:wasGivenTheTechnicalCampusStatusBy"
    ]

    generate(classes, properties, "ont_1", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])