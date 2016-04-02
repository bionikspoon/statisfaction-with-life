# statisfaction-with-life
Quick script, get data from satisfaction with life survey.

## Requirements

- python3
- `requests` or `cache_requests` from pip

## Install

```sh
git clone --depth 1 git@github.com:bionikspoon/statisfaction-with-life.git
cd statisfaction-with-life
#optional: mkvirtualenv stat -p python3
pip install -r requirements.txt
#alternative, use cache_requests: pip install -r requirements-extra.txt
python get_data.py
```


## About

Currently: 107,000 **"satisfaction with life"** survey results with demographic data.

> ###Better Life Index
> 
> Rate the topics according to their importance to you (0-5):
> 
> - Housing
> - Income
> - Jobs
> - Community
> - Education
> - Environment
> - Civic Engagement
> - Health
> - Life Satisfaction
> - Safety
> - Work-Life Balance
>
> -- http://www.oecdbetterlifeindex.org


Includes gender, age, country data, and text comments.


## Credits

Data from [http://www.oecdbetterlifeindex.org](http://www.oecdbetterlifeindex.org)