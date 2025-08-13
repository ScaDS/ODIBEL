from pydantic import BaseModel
import requests

class WikipediaSummaryResponse(BaseModel):
    """
{
   "description" : "1999 film by the Wachowskis",
   "content_urls" : {
      "desktop" : {
         "talk" : "https://en.wikipedia.org/wiki/Talk:The_Matrix",
         "revisions" : "https://en.wikipedia.org/wiki/The_Matrix?action=history",
         "page" : "https://en.wikipedia.org/wiki/The_Matrix",
         "edit" : "https://en.wikipedia.org/wiki/The_Matrix?action=edit"
      },
      "mobile" : {
         "revisions" : "https://en.m.wikipedia.org/wiki/Special:History/The_Matrix",
         "page" : "https://en.m.wikipedia.org/wiki/The_Matrix",
         "talk" : "https://en.m.wikipedia.org/wiki/Talk:The_Matrix",
         "edit" : "https://en.m.wikipedia.org/wiki/The_Matrix?action=edit"
      }
   },
   "title" : "The Matrix",
   "originalimage" : {
      "source" : "https://upload.wikimedia.org/wikipedia/en/9/94/The_Matrix.jpg",
      "width" : 220,
      "height" : 331
   },
   "description_source" : "local",
   "displaytitle" : "<i>The Matrix</i>",
   "timestamp" : "2025-07-14T06:26:09Z",
   "wikibase_item" : "Q83495",
   "extract_html" : "<p><i><b>The Matrix</b></i> is a 1999 science fiction action film written and directed by the Wachowskis. It is the first installment in the <span><i>Matrix</i> film series</span>, starring Keanu Reeves, Laurence Fishburne, Carrie-Anne Moss, Hugo Weaving, and Joe Pantoliano. It depicts a dystopian future in which humanity is unknowingly trapped inside the Matrix, a simulated reality created by intelligent machines. Believing computer hacker Neo to be \"the One\" prophesied to defeat them, Morpheus recruits him into a rebellion against the machines.</p>",
   "titles" : {
      "canonical" : "The_Matrix",
      "display" : "<i>The Matrix</i>",
      "normalized" : "The Matrix"
   },
   "type" : "standard",
   "pageid" : 30007,
   "dir" : "ltr",
   "namespace" : {
      "id" : 0,
      "text" : ""
   },
   "extract" : "The Matrix is a 1999 science fiction action film written and directed by the Wachowskis. It is the first installment in the Matrix film series, starring Keanu Reeves, Laurence Fishburne, Carrie-Anne Moss, Hugo Weaving, and Joe Pantoliano. It depicts a dystopian future in which humanity is unknowingly trapped inside the Matrix, a simulated reality created by intelligent machines. Believing computer hacker Neo to be \"the One\" prophesied to defeat them, Morpheus recruits him into a rebellion against the machines.",
   "thumbnail" : {
      "source" : "https://upload.wikimedia.org/wikipedia/en/9/94/The_Matrix.jpg",
      "width" : 220,
      "height" : 331
   },
   "tid" : "6ded5d1a-607b-11f0-a979-111cebe3b21a",
   "revision" : "1300420305",
   "lang" : "en"
}
    """
    description: str
    content_urls: dict
    title: str
    originalimage: dict
    description_source: str
    displaytitle: str
    timestamp: str
    wikibase_item: str
    extract_html: str
    extract: str
    thumbnail: dict
    tid: str
    revision: str
    lang: str


class WikipediaSummaries():

    BASE_URL = "https://en.wikipedia.org/api/rest_v1/page/summary/"

    def __init__(self):
        pass

    def get(self, title: str) -> WikipediaSummaryResponse:
        url = f"{self.BASE_URL}{title}"
        response = requests.get(url)
        return WikipediaSummaryResponse.model_validate_json(response.content.decode("utf-8"))

