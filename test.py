import boto3
from requests_html import HTML
from bs4 import BeautifulSoup
from pymongo import MongoClient
import certifi
s3 = boto3.client('s3',
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name="ap-northeast-2"
    )

objects = s3.list_objects(Bucket = 't1-tu-data')['Contents']

for i in objects:
    print(i['Key'])

title = 'yes24/51673.html'.split('/')[1].split('.')[0]
ticket_url = f'http://ticket.yes24.com/Perf/{title}'
response = s3.get_object(Bucket='t1-tu-data', Key='yes24/51675.html')
file_content = response['Body'].read().decode('utf-8')
soup = BeautifulSoup(file_content, "html.parser")

url = "mongodb+srv://summerham22:{패스워드}@cluster0.c1zjv.mongodb.net/"

client = MongoClient(url, tlsCAFile=certifi.where()) 
db = client.ham
# for i in objects:
#     filename = i["Key"]
#     print(filename)
#     response = s3.get_object(Bucket='t1-tu-data', Key=filename)
#     file_content = response['Body'].read().decode('utf-8')
#     soup = BeautifulSoup(file_content, "html.parser")
    
#     category_element = soup.select_one('.rn-location a')
#     category = category_element.text.strip() if category_element else None

#     # 단독판매 여부
#     exclusive_sales_element = soup.select_one('.rn-label')
#     exclusive_sales = exclusive_sales_element.text.strip() if exclusive_sales_element else None

#     # 공연 제목
#     title_element = soup.select_one('.rn-big-title')
#     title = title_element.text.strip() if title_element else None

#     # 공연일자
#     show_time_element = soup.select_one('.rn-product-area3 dd')
#     show_time = show_time_element.text.strip() if show_time_element else None

#     # 시작일자와 종료일자
#     date_element = soup.select_one('.ps-date')
#     if date_element:
#         date_text = date_element.text.strip()
#         start_date, end_date = map(str.strip, date_text.split('~'))  # 양쪽 공백 제거
#     else:
#         start_date = None
#         end_date = None

#     # 공연 상세 정보
#     performance_details = soup.select('.rn08-tbl td')
#     running_time = performance_details[5].text.strip() if len(performance_details) > 5 else None
#     age_rating = performance_details[4].text.strip() if len(performance_details) > 4 else None
#     performance_place = performance_details[6].text.strip() if len(performance_details) > 6 else None

#     # 가격 정보
#     price_elements = soup.select('#divPrice .rn-product-price1')
#     price = price_elements[0].text.strip() if price_elements else None

#     # 포스터 이미지 URL
#     poster_img_element = soup.select_one('.rn-product-imgbox img')
#     poster_img = poster_img_element['src'] if poster_img_element else None

#     # 혜택 및 할인 정보
#     benefits_element = soup.select_one('.rn-product-dc')
#     benefits = benefits_element.text.strip() if benefits_element else None

#     # 출연진 정보
#     performer_elements = soup.select('.rn-product-peole')
#     performer_names = [performer.text.strip() for performer in performer_elements]
#     performer_links = [performer.get('href') for performer in performer_elements]

#     # 출연진 정보가 없을 경우 빈 값 처리
#     if not performer_names:
#         performer_names.append(None)
#         performer_links.append(None)

#     # 호스팅 서비스 사업자 정보
#     hosting_provider_element = soup.select_one('.footxt p')
#     hosting_provider = hosting_provider_element.text.strip() if hosting_provider_element else None

#     # 주최자 정보
#     organizer_info_element = soup.select_one('#divPerfOrganization')
#     organizer_info = organizer_info_element.text.strip() if organizer_info_element else None


# 카테고리
category_element = soup.select_one('.rn-location a')
category = category_element.text.strip() if category_element else None

# 단독판매 여부
exclusive_sales_element = soup.select_one('.rn-label')
exclusive_sales = exclusive_sales_element.text.strip() if exclusive_sales_element else None

# 공연 제목
title_element = soup.select_one('.rn-big-title')
title = title_element.text.strip() if title_element else None

# 공연일자
show_time_element = soup.select_one('.rn-product-area3 dd')
show_time = show_time_element.text.strip() if show_time_element else None

# 시작일자와 종료일자
date_element = soup.select_one('.ps-date')
if date_element:
    date_text = date_element.text.strip()
    start_date, end_date = map(str.strip, date_text.split('~'))  # 양쪽 공백 제거
else:
    start_date = None
    end_date = None

# 공연 상세 정보
performance_details = soup.select('.rn08-tbl td')
running_time = performance_details[5].text.strip() if len(performance_details) > 5 else None
age_rating = performance_details[4].text.strip() if len(performance_details) > 4 else None
performance_place = performance_details[6].text.strip() if len(performance_details) > 6 else None

# 가격 정보
price_elements = soup.select('#divPrice .rn-product-price1')
price = price_elements[0].text.strip() if price_elements else None

# 포스터 이미지 URL
poster_img_element = soup.select_one('.rn-product-imgbox img')
poster_img = poster_img_element['src'] if poster_img_element else None

# 혜택 및 할인 정보
benefits_element = soup.select_one('.rn-product-dc')
benefits = benefits_element.text.strip() if benefits_element else None

# 출연진 정보
performer_elements = soup.select('.rn-product-peole')
performer_names = [performer.text.strip() for performer in performer_elements]
performer_links = [performer.get('href') for performer in performer_elements]

# 출연진 정보가 없을 경우 빈 값 처리
if not performer_names:
    performer_names.append(None)
    performer_links.append(None)

# 호스팅 서비스 사업자 정보
hosting_provider_element = soup.select_one('.footxt p')
hosting_provider = hosting_provider_element.text.strip() if hosting_provider_element else None

# 주최자 정보
organizer_info_element = soup.select_one('#divPerfOrganization')
organizer_info = organizer_info_element.text.strip() if organizer_info_element else None

db.users.insert_one({
    "Category": category,
   "Exclusive Sales": exclusive_sales,
   "hosts": [{"site_id": 2,
              "url":ticket_url }]


})

# 결과 출력
print(f"Category: {category}")
print(f"Exclusive Sales: {exclusive_sales}")
print(f"Title: {title}")
print(f"Show Time: {show_time}")
print(f"Start Date: {start_date}, End Date: {end_date}")
print(f"Running Time: {running_time}")
print(f"Age Rating: {age_rating}")
print(f"Performance Place: {performance_place}")
print(f"Price: {price}")
print(f"Poster Image URL: {poster_img}")
print(f"Benefits: {benefits}")
print(f"Performer Names: {performer_names}")
print(f"Performer Links: {performer_links}")
print(f"Hosting Provider: {hosting_provider}")
print(f"Organizer Info: {organizer_info}")
