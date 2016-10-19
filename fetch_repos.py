import base64
import csv
import os
from collections import defaultdict, namedtuple
from copy import copy
from functools import wraps
import json
from time import time
from urllib2 import Request, HTTPError, urlopen

HEADERS = {'Accept': "application/vnd.github.drax-preview+json",
           'Authorization': 'token %s' % os.environ["TOKEN"]}


permissive_licenses = "apache-2.0 bsd-2-clause bsd-3-clause isc mit mpl-2.0 unlicense wtfpl".split()

Repo = namedtuple("Repo", "name description license language forks stars updated".split())
RepoWithProps = namedtuple("Repo", "name description license language forks stars updated tags quality".split())


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
        

def extract_keys(repo_dict):
    name = repo_dict["full_name"]
    title = repo_dict["description"] if repo_dict["description"] else ""
    license_str = repo_dict["license"]["key"] if repo_dict["license"] else ""
    updated = repo_dict["pushed_at"][:10].replace("-", "/")
    return Repo(name=name, description=title, license=license_str, language=repo_dict["language"],
                forks=repo_dict["forks"], stars=repo_dict["stargazers_count"], updated=updated)


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


if __name__ == "__main__":
    # test = lambda_handler(None, None)
    # with open("resp.json", "w") as out_file:
    #     json.dump(dict(data=test), out_file, indent=2)
    #     out_file.close()
    with open("resp.json") as resp_json:
        resp = json.load(resp_json)
        data = [Repo(*r) for r in resp["data"]]
        with open("data/repo_props.csv", "rb") as repo_props_csv:
            repo_props = {}
            repos_dict = {}
            repo_props_reader = csv.DictReader(repo_props_csv, delimiter='|')
            for row in repo_props_reader:
                cat = ["i"] if row["cat"].strip().startswith('ip') else []
                tb = ["t"] if row["tb"] == "tb" else []
                sys = ["f"] if row["sys"] else []
                tags = "".join(cat + tb + sys)
                repo_props[row["name"]] = tags
            for repo in data:
                tags = repo_props[repo.name] if repo.name in repo_props else ""
                repos_dict[repo.name] = RepoWithProps(*repo + (tags, ""))
            with open("data/ip-libs.csv", "rb") as ip_csv:
                ip_reader = csv.DictReader(ip_csv, delimiter='|')
                ip_lib_repos = []
                ip_lib = []
                for row in ip_reader:
                    parent_repo = repos_dict[row["Repo"]]
                    descr = "{} - {}".format(row["Block"], row["Description"]) if row["Description"] else row["Block"]
                    quality = ""
                    q_str = row["Status"].strip()
                    if q_str == "SI":
                        quality = "s"
                    elif q_str == "FPGA":
                        quality = "f"
                    ip_lib_repos.append(RepoWithProps(name=parent_repo.name, description=descr,
                                                      license=parent_repo.license, language=parent_repo.language,
                                                      forks=parent_repo.forks, stars=parent_repo.stars,
                                                      updated=parent_repo.updated, tags=parent_repo.tags,
                                                      quality=quality))
                output_list = [list(r) for r in sorted((ip_lib_repos+repos_dict.values()), key=lambda x: x.name)]
                with open("data/data.json", "w") as out_file:
                    json.dump(dict(data=output_list), out_file, indent=2)
                    out_file.close()