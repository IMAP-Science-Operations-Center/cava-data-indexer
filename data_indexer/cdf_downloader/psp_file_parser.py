import ssl
import urllib.request
from typing import Dict, List, Tuple, NamedTuple

import certifi
from bs4 import BeautifulSoup


class PspFileInfo(NamedTuple):
    link: str
    name: str
    year: str


class PspFileParser:
    @staticmethod
    def get_dictionary_of_files(url: str, file_infos_by_mode=None, top_level_link=None) -> Dict[str, List[PspFileInfo]]:
        if file_infos_by_mode is None:
            file_infos_by_mode = {}
        list_of_links = PspFileParser._get_all_links(url)
        for link, element_text in list_of_links:
            if element_text.strip().endswith('.cdf'):
                year = url.split('/')[-2]
                PspFileParser._add_cdf_link_to_file_infos(element_text, link, file_infos_by_mode, top_level_link, year)
            else:
                file_infos_by_mode = PspFileParser.get_dictionary_of_files(url + link, file_infos_by_mode,
                                                                           link if top_level_link is None else top_level_link)
        return file_infos_by_mode

    @staticmethod
    def _add_cdf_link_to_file_infos(element_text: str, link: str, file_infos_by_mode: Dict[str, List[PspFileInfo]],
                                    top_level_link: str, year: str):
        new_link = PspFileInfo(link, element_text, year)
        if top_level_link in file_infos_by_mode:
            file_infos_by_mode[top_level_link].append(new_link)
        else:
            file_infos_by_mode[top_level_link] = [new_link]

    @staticmethod
    def _get_all_links(url: str) -> List[Tuple[str, str]]:
        context = ssl.create_default_context(cafile=certifi.where())
        list_of_links = []
        with urllib.request.urlopen(url, context=context) as page:
            soup = BeautifulSoup(page, 'html.parser')
            for a_tag in soup.select("td > a")[1:]:
                link = a_tag.get('href')
                element_text = a_tag.text
                list_of_links.append((link, element_text))
        return list_of_links
