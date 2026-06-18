import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Background",
        "dbo:Channel",
        "dbo:Cinematography",
        "dbo:Club",
        "dbo:Industry",
        "dbo:Occupation",
        "dbo:Service",
        "dbo:Station",
        "dbo:Type",
        "dbo:City",
        "dbo:Country",
        "dbo:Language",
        "dbo:Organisation",
        "dbo:Place",
        "dbo:Company",
        "dbo:Person",
        "dbo:Artist",
        "dbo:Film"
    ]

    properties = [
        "dbo:activeYearsStartYear",
        "dbo:background",
        "dbo:birthDate",
        "dbo:birthName",
        "dbo:birthPlace",
        "dbo:birthYear",
        "dbo:broadcastedBy",
        "dbo:budget",
        "dbo:child",
        "dbo:cinematography",
        "dbo:club",
        "dbo:deathDate",
        "dbo:deathPlace",
        "dbo:deathYear",
        "dbo:director",
        "dbo:distributor",
        "dbo:editing",
        "dbo:editor",
        "dbo:formerName",
        "dbo:foundedBy",
        "dbo:foundingYear",
        "dbo:gross",
        "dbo:headquarter",
        "dbo:imdbId",
        "dbo:industry",
        "dbo:iso6391Code",
        "dbo:iso6392Code",
        "dbo:keyPerson",
        "dbo:language",
        "dbo:location",
        "dbo:musicComposer",
        "dbo:occupation",
        "dbo:owner",
        "dbo:producer",
        "dbo:releaseDate",
        "dbo:runtime",
        "dbo:service",
        "dbo:sisterStation",
        "dbo:spokenIn",
        "dbo:spouse",
        "dbo:starring",
        "dbo:timeshiftChannel",
        "dbo:type",
        "dbo:writer"
    ]

    generate(classes, properties, "ont_19", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])