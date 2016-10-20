import base64
import csv
import os
from collections import defaultdict
from collections import namedtuple
from functools import wraps
import json
from operator import itemgetter
from time import time
from urllib2 import Request, HTTPError, urlopen

from datetime import datetime

HEADERS = {'Accept': "application/vnd.github.drax-preview+json",
           'Authorization': 'token %s' % os.environ["TOKEN"]}

HDL_LANGUAGES = ["SystemVerilog", "Verilog", "VHDL"]
TS_PATH = "data/languages-cache/ts.json"

permissive_licenses = "apache-2.0 bsd-2-clause bsd-3-clause isc mit mpl-2.0 unlicense wtfpl".split()

Repo = namedtuple("Repo", "name description license languages forks stars updated tags".split())
RepoWithTags = namedtuple("RepoWithProps", "name description license languages forks stars updated tags quality".split())


class IP_Block(tuple):
    'IP_Block(repo, description, quality)'

    __slots__ = ()

    _fields = ('repo', 'description', 'quality')

    def __new__(_cls, repo, block, description, quality):
        'Create new instance of IP_Block(repo, description, quality)'
        descr = "{} - {}".format(block, description) if description else block
        return tuple.__new__(_cls, (repo, descr, quality))

    repo = property(itemgetter(0), doc='Alias for field number 0')

    description = property(itemgetter(1), doc='Alias for field number 1')

    quality = property(itemgetter(2), doc='Alias for field number 2')


def timed(f):
    @wraps(f)
    def wrapper(*args, **kwds):
        start = time()
        fun_result = f(*args, **kwds)
        elapsed = time() - start
        print "%s took %f seconds to finish" % (f.__name__, elapsed)
        return fun_result
    return wrapper


def parse_headers(headers):
    if 'link' in  headers:
        next_link = [s for s in headers["link"].split(",") if 'rel="next"' in s]
        if next_link:
            return next_link[0].split(";")[0][1:-1]


def get(first_page_url):
    headers = HEADERS
    url = first_page_url
    result = []
    try:
        while url:
            request = Request(url, headers=headers)
            response = urlopen(request)
            url = parse_headers(response.headers)
            data = json.load(response)
            if isinstance(data, list):
                result.extend(data)
            else:
                return data
        return result
    except HTTPError, e:
        print("Get from {} failed: {}!".format(url, e))
        return {}


def get_repo(repo_name):
    url = "https://api.github.com/repos/{}".format(repo_name)
    return get(url)


def get_user_repos(user_name):
    return get("https://api.github.com/users/{}/repos".format(user_name))


@timed
def get_file_contents(url):
    contents_doc = get(url)
    if contents_doc:
        content = contents_doc["content"]
        return sorted(json.loads(base64.decodestring(content)))


def extract_repos_data(list_of_repo_dicts):
    repos = []
    for repo in list_of_repo_dicts:
        if repo["language"] in ['SystemVerilog', 'Verilog', 'VHDL']:
            if not repo['fork'] or repo['stargazers_count'] > 0:
                repos.append(extract_keys(repo))
    return repos


def smart_truncate(content, length=96, suffix='...'):
    if len(content) <= length:
        return content
    else:
        return ' '.join(content[:length+1].split(' ')[0:-1]) + suffix


def extract_keys(repo_dict):
    name = repo_dict["full_name"]
    title = smart_truncate(repo_dict["description"]) if repo_dict["description"] else ""
    license_str = repo_dict["license"]["key"] if repo_dict["license"] else ""
    updated = repo_dict["pushed_at"][:10].replace("-", "/")
    return Repo(name=name, description=title, license=license_str, languages=[repo_dict["language"]],
                forks=repo_dict["forks"], stars=repo_dict["stargazers_count"], updated=updated,
                tags=["fork"] if repo_dict["fork"] else [])


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


@timed
def get_users_repo_chunk(users_chunk):
    chunk_result = []
    for user in users_chunk:
        user_repos = get_user_repos(user)
        chunk_result.extend(extract_repos_data(user_repos))
    return chunk_result


@timed
def get_repo_chunk(repos_chunk):
    chunk_result = []
    for repo_name in repos_chunk:
        repo = get_repo(repo_name)
        if repo:
            chunk_result.append(extract_keys(repo))
    return chunk_result


@timed
def get_languages(for_repo_with_name):
    headers = HEADERS
    url = "https://api.github.com/repos/{}/languages".format(for_repo_with_name)
    try:
        request = Request(url, headers=headers)
        response = urlopen(request)
        data = json.load(response)
        return data
    except HTTPError, e:
        print("Get from {} failed: {}!".format(url, e))
        return {}


def update_and_merge_languages(repos):
    # load timestamp of last update
    with open(TS_PATH) as last_ts_file:
        ts = json.load(last_ts_file)
        cache_ts = ts[0]
        updated_repos = []
        processed = set()
        for repo in repos:
            fn = os.path.join("data", "languages-cache", "{}.json".format(repo.name.replace("/", ":")))
            if repo.updated > cache_ts and repo.name not in processed or not os.path.exists(fn):
                print("fetching languages for repo: {}".format(repo.name))
                languages = get_languages(repo.name)
                with open(fn, "w") as languages_json:
                    json.dump(languages, languages_json)
                    languages_json.close()
                processed.add(repo.name)
            else:
                with open(fn) as languages_json:
                    languages = json.load(languages_json)
            repo_hdl_languages = [l for l in languages.keys() if l in HDL_LANGUAGES]
            repo_dict = repo._asdict()
            repo_dict["languages"] = repo_hdl_languages or repo.languages
            updated_repos.append(Repo(**repo_dict))
            if not repo_hdl_languages:
                print("No match in {}".format(repo.name))
        today = datetime.today()
        ts = "{}/{}/{}".format(today.year, today.month, today.day)
        # Create new timestamp
        with open(TS_PATH, "w") as new_ts_file:
            json.dump([ts], new_ts_file)
            new_ts_file.close()
        return updated_repos


def merge_repo_tags(repos):
    with open("data/repo_tags.csv", "rb") as repo_props_csv:
        repo_props = defaultdict(list)
        repo_props_reader = csv.DictReader(repo_props_csv, delimiter='|')
        merged_repos = []
        for row in repo_props_reader:
            repo_name = row["repo"]
            tags = row["tags"].split(",")
            repo_props[repo_name] = tags
            # cat = ["i"] if row["cat"].strip().startswith('ip') else []
            # tb = ["t"] if row["tb"] == "tb" else []
            # sys = ["f"] if row["sys"] else []
            # tags = "".join(cat + tb + sys)
            # repo_props[row["name"]] = tags
        for repo in repos:
            repo_dict = repo._asdict()
            repo_dict["tags"] = repo_props[repo.name] + repo.tags
            merged_repos.append(RepoWithTags(quality="", **repo_dict))
    return merged_repos


def expand_ip_blocks(repos_with_tags):
    with open("data/ip-libs.csv", "rb") as ip_csv:
        ip_reader = csv.DictReader(ip_csv, delimiter='|')
        ip_lib = defaultdict(list)
        expanded_ip_blocks = []
        for row in ip_reader:
            ip_lib[row["repo"]].append(IP_Block(**row))

        for parent_repo in repos_with_tags:
            if parent_repo.name in ip_lib:
                for child_repo in ip_lib[parent_repo.name]:
                    expanded_ip_blocks.append(RepoWithTags(name=parent_repo.name, description=child_repo.description,
                                                           license=parent_repo.license, languages=parent_repo.languages,
                                                           forks=parent_repo.forks, stars=parent_repo.stars,
                                                           updated=parent_repo.updated, tags=parent_repo.tags,
                                                           quality=child_repo.quality))
    return expanded_ip_blocks


@timed
def lambda_handler(event, context):
    data_base_url = "https://api.github.com/repos/berndca/discover_fossi/contents/data"
    repos = get_file_contents(data_base_url + "/repos.json?ref=gh-pages")
    users = get_file_contents(data_base_url + "/users.json?ref=gh-pages")
    all_repos = []
    for chunk in chunks(users, len(users)/7):
        all_repos.extend(get_users_repo_chunk(chunk))
    for chunk in chunks(repos, len(repos)/4):
        all_repos.extend(get_repo_chunk(chunk))
    return all_repos


def load_extracted_repos():
    with open("resp.json") as resp_json:
        resp = json.load(resp_json)
        return [Repo(*r) for r in resp["data"]]


if __name__ == "__main__":
    # resp = lambda_handler(None, None)
    # with open("resp.json", "w") as out_file:
    #     json.dump(dict(data=resp), out_file, indent=2)
    #     out_file.close()

    resp_data = load_extracted_repos()
    languages_data = update_and_merge_languages(resp_data)
    tagged_data = merge_repo_tags(languages_data)
    ip_blocks = expand_ip_blocks(tagged_data)
    combined_and_sorted = sorted((tagged_data+ip_blocks), key=lambda x: x.name)

    with open("data/data.json", "w") as out_file:
                json.dump(dict(data=[list(r) for r in combined_and_sorted]), out_file, indent=2)
                out_file.close()