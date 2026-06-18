import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:AcademicDiscipline",
        "dbo:MediaType",
        "dbo:Organisation",
        "dbo:Genre",
        "dbo:Place",
        "dbo:City",
        "dbo:Company",
        "dbo:Country",
        "dbo:Person",
        "dbo:WrittenWork"
    ]

    properties = [
        "dbo:LCCN_number",
        "dbo:abbreviation",
        "dbo:academicDiscipline",
        "dbo:affiliation",
        "dbo:almaMater",
        "dbo:author",
        "dbo:birthDate",
        "dbo:birthPlace",
        "dbo:capital",
        "dbo:city",
        "dbo:codenCode",
        "dbo:country",
        "dbo:doctoralAdvisor",
        "dbo:editor",
        "dbo:ethnicGroup",
        "dbo:firstPublicationYear",
        "dbo:followedBy",
        "dbo:founder",
        "dbo:frequency",
        "dbo:genre",
        "dbo:headquarter",
        "dbo:impactFactor",
        "dbo:influencedBy",
        "dbo:isbnNumber",
        "dbo:issnNumber",
        "dbo:language",
        "dbo:leader",
        "dbo:leaderTitle",
        "dbo:libraryofCongressClassification",
        "dbo:literaryGenre",
        "dbo:mediaType",
        "dbo:nationality",
        "dbo:notableWork",
        "dbo:numberOfPages",
        "dbo:oclcNumber",
        "dbo:parentCompany",
        "dbo:precededBy",
        "dbo:president",
        "dbo:publisher",
        "dbo:regionServed",
        "dbo:releaseDate",
        "dbo:residence",
        "dbo:spokenIn",
        "dbo:state"
    ]

    generate(classes, properties, "ont_14", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])