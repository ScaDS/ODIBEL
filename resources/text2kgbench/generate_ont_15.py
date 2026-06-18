import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Company",
        "dbo:Demonym",
        "dbo:Ground",
        "dbo:Language",
        "dbo:Season",
        "dbo:State",
        "dbo:Tenant",
        "dbo:Athlete",
        "dbo:League",
        "dbo:Place",
        "dbo:City",
        "dbo:Country",
        "dbo:Person",
        "dbo:SportsTeam"
    ]

    properties = [
        "dbo:birthPlace",
        "dbo:capital",
        "dbo:chairmanTitle",
        "dbo:champions",
        "dbo:city",
        "dbo:club",
        "dbo:country",
        "dbo:demonym",
        "dbo:fullName",
        "dbo:ground",
        "dbo:isPartOf",
        "dbo:language",
        "dbo:leader",
        "dbo:league",
        "dbo:location",
        "dbo:manager",
        "dbo:mayor",
        "dbo:nickname",
        "dbo:numberOfMembers",
        "dbo:operator",
        "dbo:owner",
        "dbo:season",
        "dbo:state",
        "dbo:tenant"
    ]

    generate(classes, properties, "ont_15", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])