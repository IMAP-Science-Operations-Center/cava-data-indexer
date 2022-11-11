import urllib.request
from typing import Dict, List, Tuple, NamedTuple

from bs4 import BeautifulSoup


class PspFileInfo(NamedTuple):
    link: str
    name: str
    year: str


class PspFileParser:

    @staticmethod
    def get_dictionary_of_files(url: str, list_of_files=None, top_level_link=None) -> Dict:
        if list_of_files is None:
            list_of_files = {}
        list_of_links = PspFileParser._get_all_links(url)
        for link, element_text in list_of_links:
            if element_text.strip().endswith('.cdf'):
                year = url.split('/')[-2]
                PspFileParser._add_cdf_link_to_list(element_text, link, list_of_files, top_level_link, year)
            else:
                list_of_files = PspFileParser.get_dictionary_of_files(url + link, list_of_files,
                                                                      link if top_level_link is None else top_level_link)
        return list_of_files

    @staticmethod
    def _add_cdf_link_to_list(element_text: str, link: str, list_of_files: List[str], top_level_link: str, year: str):
        new_link = PspFileInfo(link, element_text, year)
        if top_level_link in list_of_files:
            list_of_files[top_level_link].append(new_link)
        else:
            list_of_files[top_level_link] = [new_link]

    @staticmethod
    def _get_all_links(url: str) -> List[Tuple[str, str]]:
        list_of_links = []
        with urllib.request.urlopen(url) as page:
            soup = BeautifulSoup(page, 'html.parser')
            for a_tag in soup.select("td > a")[1:]:
                link = a_tag.get('href')
                element_text = a_tag.text
                list_of_links.append((link, element_text))
        return list_of_links
