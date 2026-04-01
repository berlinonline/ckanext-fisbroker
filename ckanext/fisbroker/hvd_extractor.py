'''
Code for extracting HVD-category information from geospatial metadata (ISO 19139),
as it is found in the getrecordbyid() responses from a CSW. The pattern is as
follows:

```xml
<gmd:MD_Metadata xmlns:gmd="http://www.isotc211.org/2005/gmd">
<!-- HVD Thesaurus-Based Keywords Section -->
    <gmd:descriptiveKeywords>
        <gmd:MD_Keywords>
            <!-- Category Keyword -->
            <gmd:keyword>
                <gmx:Anchor xlink:href="http://data.europa.eu/bna/c_dd313021">Earth observation and environment</gmx:Anchor>
            </gmd:keyword>
            <!-- Thesaurus Reference -->
            <gmd:thesaurusName>
                <gmd:CI_Citation>
                    <gmd:title>
                        <gmx:Anchor xlink:href="http://data.europa.eu/bna/asd487ae75">High-value dataset categories</gmx:Anchor>
                    </gmd:title>
                    <gmd:date>
                        <gmd:CI_Date>
                            <gmd:date>
                                <gco:Date>2023-09-05</gco:Date>
                            </gmd:date>
                            <gmd:dateType>
                                <gmd:CI_DateTypeCode codeList="http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/ML_gmxCodelists.xml#CI_DateTypeCode" codeListValue="publication"/>
                            </gmd:dateType>
                        </gmd:CI_Date>
                    </gmd:date>
                </gmd:CI_Citation>
            </gmd:thesaurusName>
        </gmd:MD_Keywords>
    </gmd:descriptiveKeywords>
</gmd:MD_Metadata>
```

see https://dataeuropa.gitlab.io/data-provider-manual/hvd/annotation_in_geometadata
'''

from lxml import etree

NS = {
    "gmd": "http://www.isotc211.org/2005/gmd",
    "gmx": "http://www.isotc211.org/2005/gmx",
    "xlink": "http://www.w3.org/1999/xlink",
}

HVD_PREFIX = "http://data.europa.eu/bna/"
HVD_THESAURUS_URI = f"{HVD_PREFIX}asd487ae75"

# pattern for finding the correct gmd:descriptiveKeywords element
# and extracting the keyword anchors from it
HVD_PATTERN = f"""
//gmd:descriptiveKeywords[
  gmd:MD_Keywords
    /gmd:thesaurusName
    /gmd:CI_Citation
    /gmd:title
    /gmx:Anchor[@xlink:href="{HVD_THESAURUS_URI}"]
]
/gmd:MD_Keywords
/gmd:keyword
/gmx:Anchor[@xlink:href]
"""

def extract_hvd_categories(tree: etree) -> list:
    """Extract a list of HVD categories from an ISO 19139
    geospatial metadata XML document.

    Args:
        tree (etree): the XML document

    Returns:
        list: A list of dicts with 'uri' and 'label' keys
    """

    anchors = tree.xpath(HVD_PATTERN, namespaces=NS)
    results = []

    for anchor in anchors:
        uri = anchor.get("{http://www.w3.org/1999/xlink}href")
        label = anchor.text.strip() if anchor.text else None
        if uri.startswith("http://data.europa.eu/bna/"):
            results.append({"uri": uri, "label": label})
    
    return results

