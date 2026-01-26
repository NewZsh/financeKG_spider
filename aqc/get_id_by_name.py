from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import json
import re
from lxml import etree
from urllib.parse import quote

cookie = 'BIDUPSID=716FD303B838AEC86DCB0EA55D681A4E; PSTM=1713347822; BAIDUID=A6570B2260C54761BB9CA2D1924DF756:SL=0:NR=10:FG=1; MCITY=-303%3A; BDPPN=b9f2a3e5cc6289195c8cf3cc05e04e0b; _t4z_qc8_=xlTM-TogKuTwum0rWXRh1t5dnA4l%2AolYIgmd; log_guid=25db5fade5b9ef29fcf236733412c707; login_type=passport; _j47_ka8_=57; H_PS_PSSID=60335_60364_60346; H_WISE_SIDS=60335_60364_60346; H_WISE_SIDS_BFESS=60335_60364_60346; ZFY=ErCLKDEf3ODyWnA1ouMJZPHB62B3OtsA7LiYtwuYDTk:C; BAIDUID_BFESS=A6570B2260C54761BB9CA2D1924DF756:SL=0:NR=10:FG=1; __bid_n=18f1ad96183745660bcc58; BDUSS=WpMUzBkckdmVW9jOXdscnZ4b1ZxQnZOM1pWflhWZW85cmQ5VENNVGxhNC1ucVJtSVFBQUFBJCQAAAAAAAAAAAEAAAA9H0Yhc2loZW5nMjAxMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD4RfWY-EX1ma; BDUSS_BFESS=WpMUzBkckdmVW9jOXdscnZ4b1ZxQnZOM1pWflhWZW85cmQ5VENNVGxhNC1ucVJtSVFBQUFBJCQAAAAAAAAAAAEAAAA9H0Yhc2loZW5nMjAxMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD4RfWY-EX1ma; BA_HECTOR=8125048n2l21248gal012k84239rhm1j828g41u; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; __jdg_yd=lTM-TogKuTwn0mXPcGT6ZVVFuHW38DCTe0n%2AunCuF4HATROcLz85%2A11rTQiOxB%2Ay6WXUMsu75KutKajSXPA4t8VPYPg3XrQKwawlNVlsRTcaNREJHgTa-g; log_chanel=; Hm_lvt_ad52b306e1ae4557f5d3534cce8f8bbf=1719197287,1719734698,1719758568; log_first_time=1719758568472; _fb537_=xlTM-TogKuTw2H7fmcLSfjod7DH4DMwPiB30BIREBsLhpbVTd%2AtPNbQmd; ab171975600=188ac034858c8978c27ebaeb2dd703781719758621725; Hm_lpvt_ad52b306e1ae4557f5d3534cce8f8bbf=1719758622; ab_sr=1.0.1_OGE4Yzg0NGVhZDY0MzdiZTIxZDFmMmZhYzgwMWQ1ODc3NDA3Yzg3ODk4ODkzMGViZDQxN2MxNDE5MGM3ODJiYmU0YWExMTg0Zjk0NWQyNDMwNDI0MGFjYjc4NWJhZDg4NGRmYzFkMmQ4MWZhMjY4ZTdhZDczY2JkMDE4YWIzODg4ODgyY2Y4ODg3YzgzNTE3NzkxMGJiMWI1YmQxZjVlMg==; _s53_d91_=7f7a5e7026de66906ddf61fd683ba073e3153b1fb9e633a75d4723b4a091db1ed6cafae1553b7e1d6cb0994b2e8b5054d0750d80ea412627c6868962381d261ed93e813d660a8d801c16ac47d375b3e926fa98d123fe43fff31b7887c5f2dc568cd9fbf23b69da8d0b7e92dbdac0494c4b24eae23bb648c8ef54a900cd7514e0c7d1292a8b2b6b2428f0038d2dd33e0a89e6a83d6e7506e0231287be5442f35e9b01fbab5effb4af576d7900ea58a79e4e63b5b0ba1de0e0161262ef4df9a53c056ba2fd2a0ada99a31ab5dcd7f9231d; _y18_s21_=5502eaf9; log_last_time=1719759072967; RT="z=1&dm=baidu.com&si=142ff5f7-e988-49bc-b831-b58d1682d636&ss=ly1ntz6i&sl=3&tt=6oz&bcn=https%3A%2F%2Ffclog.baidu.com%2Flog%2Fweirwood%3Ftype%3Dperf&ld=17mc&ul=c0na"'
useragent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
referer = 'https://aiqicha.baidu.com/'

chrome_options = Options()
chrome_options.add_argument("--mute-audio")  # 将浏览器静音
chrome_options.add_experimental_option("detach", True)  # 当程序结束时，浏览器不会关闭
chrome_options.add_argument(f'--user-agent="{useragent}"')
chrome_options.add_argument(f'--cookie="{cookie}"')
chrome_options.add_argument(f'--referer="{referer}"')
# '/home/zsh/Downloads/chromedriver_linux64/chromedriver'
browser = webdriver.Chrome(options=chrome_options)

lines = open('aqcid_by_name_result.txt').readlines()
name_ID = {}
for line in lines:
    if len(line.strip()) > 2:
        if (line[0].isdigit() and line[1] == ':')  or (line[0].isdigit() and line[1].isdigit() and line[2] == ':'):
            idx, ID, name = line.strip().split(' ', 2)
            name_ID[name] = ID
print(f"{len(name_ID)} names loaded.")

for name in open('names.txt'):
    print(name.strip())
    if name.strip() in name_ID:
        print(f'{name.strip()} already in the list. skip.')
        continue

    try:
        browser.get(f'https://aiqicha.baidu.com/')

        # wait until the input element with id = `aqc-search-input` is loaded
        while True:
            try:
                search_input = browser.find_element(By.ID, 'aqc-search-input')
                break
            except:
                time.sleep(5)

        # input the name
        search_input.send_keys(name.strip())
        # click the search button with class name `search-btn`
        search_btn = browser.find_element(By.CLASS_NAME, 'search-btn')
        search_btn.click()

        time.sleep(2)
        xml  = etree.HTML(browser.page_source)
        part = xml.xpath('//body/script')[0].xpath('.//text()')
        if len(part) == 0:
            print('fail to search (empty content due to anti-spider) q=%s' % name)
            continue

        text = re.sub(r'<[^>]*?>', '', part[0].split('\n')[0].split('window.pageData = ', 1)[1][:-1])
        res  = json.loads(text)['result']

        target_task = None
        relate_task = set()
        resultList = res.get('resultList', [])
        if len(resultList) == 0:
            print('no result for %s' % name)
            continue

        for i, result in enumerate(resultList):
            # print(result)
            ID = result['pid']
            name = result['entName']
            name_ID[name] = ID
            print(f'{i+1}: {ID} {name}')
    except:
        print('fail to search (due to anti-spider) q=%s' % name)
        time.sleep(5)
        continue