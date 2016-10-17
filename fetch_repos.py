import base64
import os
from functools import wraps
import json
from time import time
from urllib2 import Request, HTTPError, urlopen

HEADERS = {'Accept': "application/vnd.github.drax-preview+json",
           'Authorization': 'token %s' % os.environ["TOKEN"]}


def timed(f):
    @wraps(f)
    def wrapper(*args, **kwds):
        start = time()
        result = f(*args, **kwds)
        elapsed = time() - start
        print "%s took %f seconds to finish" % (f.__name__, elapsed)
        return result
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
    result = []
    for repo in list_of_repo_dicts:
        if repo["language"] in ['SystemVerilog', 'Verilog', 'VHDL']:
            if not repo['fork'] or repo['stargazers_count'] > 0:
                result.append(extract_keys(repo))
    return result
        

def extract_keys(repo_dict):
    name = repo_dict["full_name"]
    description = repo_dict["description"] if repo_dict["description"] else ""
    return [
        name,
        description,
        repo_dict["license"]["key"] if repo_dict["license"] else "",
        repo_dict["language"],
        repo_dict["forks"],
        repo_dict["stargazers_count"],
        repo_dict["updated_at"][:10].replace("-", "/")
    ]


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
    result = []
    for chunk in chunks(users, len(users)/7):
        result.extend(get_users_repo_chunk(chunk))
    for chunk in chunks(repos, len(repos)/4):
        result.extend(get_repo_chunk(chunk))
    return result


if __name__ == "__main__":
    test = lambda_handler(None, None)
    with open("resp.json", "w") as out_file:
        json.dump(dict(data=test), out_file, indent=2)
        out_file.close()

