from generate import generate
classes = [
    "dbo:Election",
    "dbo:GovernmentAgency",
    "dbo:GovernmentCabinet",
    "dbo:GovernmentType",
    "dbo:Ideology",
    "dbo:LegalCase",
    "dbo:SupremeCourtOfTheUnitedStatesCase",
    "dbo:Legislature",
    "dbo:PoliticalConcept",
    "dbo:PoliticalParty",
    "dbo:SystemOfLaw",
    "dbo:Treaty"
]

generate(classes, "politics")