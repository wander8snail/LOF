import time
import requests
import pandas as pd
import pytz
import json
import configparser
from datetime import datetime


class RUN:
    def __init__(self):
        self.cp = configparser.ConfigParser()
        self.cp.read("config.cfg", encoding="utf8")
        if "LOF" and "content" not in list(self.cp.sections()):
            raise Exception("Please create config.cfg first")
        self.content = self.cp._sections['content']
        self.data_info = self.cp._sections['data_info']
        self.LOFList = self.cp.get('LOF','LOFList').strip('[').strip(']').split(', ')
        self.LOFList.sort()

        self.FundList = self.cp.get('LOF','FundList').strip('[').strip(']').split(', ')
        self.FundList.sort()

        self.stra_dic = eval(self.cp.get('DANJUAN','stra_dic'))

        self.disLimit = self.cp.getfloat('LOF', 'disLimit')
        self.preLimit = self.cp.getfloat('LOF', 'preLimit')
        if self.disLimit < 0.: self.disLimit = 0.
        if self.preLimit > 0.: self.preLimit = 0.
        self.apiKey = self.cp.get('LOF', 'apiKey')


    def getLOFInfo(self, id):
        sess = requests.Session()
        header = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36",}
        sess.headers.update(header)
        urlBase = "https://www.jisilu.cn/data/lof/detail/"
        urlLOF = "https://www.jisilu.cn/data/lof/stock_lof_list/?___jsl=LST___t="

        r = sess.get(urlLOF + str(int(time.time())*1000))
        if r.status_code == 200:
            r = r.json()
        else:
            return
        rows = [row["cell"] for row in r["rows"] if row["id"] in self.LOFList]
        res = []
        for row in rows:
            discount_rt = float(row["discount_rt"][:-1])
            if discount_rt >= self.disLimit or discount_rt <= self.preLimit:
                s = {}
                for key, value in self.content.items():
                    s[key] = row[value] if value != "fund_id" else "".join(["[", row[value], "](", urlBase, row[value], ")"])
                res.append(s)
        return res

    # 获取历史信息
    def crawler_nav(self, strategy, page):

        url = 'https://danjuanfunds.com/djapi/plan/nav/history/{}?size=60&page={}'.format(strategy, page)
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'dnt': '1',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) '
                          'Version/6.0 Mobile/10A5376e Safari/8536.25',
        }

        try:
            sess = requests.session()
            resp = sess.get(url=url, headers=headers)
            js = json.loads(resp.text)
            data = js['data']
            items = data['items']

            return pd.DataFrame(items)

        except:
            pass

    # 获取策略持仓信息
    def crawler_pos(self, strategy):
        url = 'https://danjuanfunds.com/djapi/plan/position/detail?plan_code={}'.format(strategy)
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'dnt': '1',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) '
                          'Version/6.0 Mobile/10A5376e Safari/8536.25',
        }

        try:
            sess = requests.session()
            resp = sess.get(url=url, headers=headers)
            js = json.loads(resp.text)
            data = js['data']
            items = data['items']

            return pd.DataFrame(items)

        except:
            pass

    # 获取基金最新估值
    def crawler_est_nav(self, fund):
        url = 'https://danjuanfunds.com/djapi/fund/estimate-nav/{}'.format(fund)
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'dnt': '1',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) '
                          'Version/6.0 Mobile/10A5376e Safari/8536.25',
        }

        try:
            sess = requests.session()
            resp = sess.get(url=url, headers=headers)
            js = json.loads(resp.text)
            data = js['data']
            items = data['items']

            df = pd.DataFrame(items)
            df['time'] = df['time'].map(lambda x: pd.Timestamp(x, unit="ms", tz='Asia/Shanghai'))
            return df.iloc[-1, :]

        except:
            pass


    # 获取关注基金阶段涨幅,
    def get_net_rate(self):
        sess = requests.Session()
        header = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36",}
        sess.headers.update(header)

        res = []
        for fund in self.FundList:
            url_fund = 'http://fund.10jqka.com.cn/data/client/myfund/{}'.format(fund)
            r = sess.get(url_fund)
            if r.status_code == 200 :
                r = r.json()
            else:
                return

            s = {}
            row = r['data'][0]
            for key, value in self.data_info.items():
                if abs(float(row['rate'])) >= 2 or abs(float(row['week'])) >= 5:
                    s[key] = row[value] if value != "code" else "".join(["[", row[value], "](http://fund.10jqka.com.cn/", fund, ")"])

            if s:
                res.append(s)

        return res


    def fd_est(self, df_pos):
        df_pos['time'], df_pos['ext_nav'] = pd.Timestamp.now(), 0
        for i, r in df_pos.iterrows():
            if r['type'] != 'qdii':
                time, nav, perc = self.crawler_est_nav(r['fd_code'])
                df_pos.loc[i, 'ext_nav'] = perc
        return df_pos

    def strategy_ext_per(self, strategy):
        df_pos = self.crawler_pos(strategy)
        df_pos = self.fd_est(df_pos)
        df_pos['s'] = df_pos['percent'] * df_pos['ext_nav']
        return str(round(df_pos['s'].sum() / 100, 3))


    def md_lof(self, info):
        if not info: return
        res = ["| " + " | ".join(list(info[0])) + " |"]
        res.append("| " + " :---: | " * (len(info[0]) - 1) + " :---: |")
        for i in info:
            res.append("| " + " | ".join(list(i.values())) + " |")
        res = "\n".join(res)
        return res


    def message(self, key, title, body):
        msg_url = "https://sc.ftqq.com/{}.send?text={}&desp={}".format(key, title, body)
        requests.get(msg_url)


    def main(self):
        nowt = datetime.now(tz=pytz.timezone("Asia/Shanghai")).strftime("%m-%d %H:%M")

#         if nowt[6:8] == '08' or nowt[6:8] == '21':
        fund_info = self.get_net_rate()
        if fund_info:
            md = self.md_lof(fund_info)
            self.message(self.apiKey, "基金-走势: " + nowt, md)
            print(md)
                
#         elif nowt[6:8] == '14' or nowt[6:8] == '13':
        info_lof = self.getLOFInfo(id)
        if len(info_lof):
            md = self.md_lof(info_lof)
            self.message(self.apiKey, "LOF-溢价: " + nowt, md)

        msg = []
        for k, v in self.stra_dic.items():
            prep = self.strategy_ext_per(k)
            msg.append({'策略':v, '涨幅估值':prep})
        if msg:
            md = self.md_lof(msg)
            self.message(self.apiKey, "蛋卷-策略: " + nowt, md)


if __name__ == "__main__":
    lof = RUN()
    lof.main()
