import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Anthem",
        "dbo:College",
        "dbo:EthnicGroup",
        "dbo:Language",
        "dbo:League",
        "dbo:Season",
        "dbo:Timezone",
        "dbo:Date",
        "dbo:Place",
        "dbo:Year",
        "dbo:Team",
        "dbo:Person",
        "dbo:Club",
        "dbo:Athlete"
    ]

    properties = [
        "dbo:activeYearsStartYear",
        "dbo:anthem",
        "dbo:areaTotal",
        "dbo:birthDate",
        "dbo:birthPlace",
        "dbo:birthYear",
        "dbo:chairman",
        "dbo:city",
        "dbo:club",
        "dbo:coach",
        "dbo:college",
        "dbo:currentclub",
        "dbo:currentteam",
        "dbo:deathPlace",
        "dbo:debutTeam",
        "dbo:draftPick",
        "dbo:draftRound",
        "dbo:draftTeam",
        "dbo:draftYear",
        "dbo:ethnicGroup",
        "dbo:formerTeam",
        "dbo:foundingDate",
        "dbo:generalManager",
        "dbo:ground",
        "dbo:height",
        "dbo:isPartOf",
        "dbo:language",
        "dbo:leader",
        "dbo:leaderTitle",
        "dbo:league",
        "dbo:manager",
        "dbo:owner",
        "dbo:season",
        "dbo:timeZone",
        "dbo:utcOffset",
        "dbo:weight",
        "dbo:youthclub"
    ]

    generate(classes, properties, "ont_5", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])