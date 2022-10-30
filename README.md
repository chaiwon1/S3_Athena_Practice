<h1>Simple ETL Project</h1>

1. 약 380만개의 `event_data`와 약 20만개의 `attribution_data`가 csv파일로 로컬에 존재하고, 이것을 S3에 업로드 한다.
2. S3에서 csv 파일을 읽어들여와 parquet 파일로 변환 후 다시 S3에 적재하는 ETL을 실시한다. 
3. S3에는 `server_datetime`으로 partitioning 되어있으며, Athena에서 분석쿼리를 실시한다. 

사용스킬 : `Python`, `AWS S3`, `AWS Athena`

<br>

<h2>데이터 소개</h2>

<h4>event_data : 사용자 행동을 기록한 데이터</h4>

![image](https://user-images.githubusercontent.com/95471902/198695166-ba4e4ea8-f255-449f-9300-ba9875b41a8d.png)

<h4>attribution_data : 광고를 통합 유입을 기록한 데이터</h4>

![image](https://user-images.githubusercontent.com/95471902/198695935-65e5e25e-3223-49a9-a92a-0bb07b7b8c16.png)

<br>

<h2>프로젝트 아키텍쳐</h2>
<img width="907" alt="archetecture" src="https://user-images.githubusercontent.com/95471902/198686964-d69b1dc8-15c1-49e6-94b9-b2212689370f.png">

<br>

<h2>실행 결과</h2>

<h4>ETL 실행 결과</h4>

![캡처12](https://user-images.githubusercontent.com/95471902/198697662-9585809b-3a0a-446c-af9f-8f2f1eb1cdf8.png)

<h4>쿼리 실행 결과</h4>

![query-1](https://user-images.githubusercontent.com/95471902/198697799-0476a02a-bd9e-4b25-8c12-042aed23beda.PNG)
