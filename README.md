🚇 Seoul Metro Congestion Data Pipeline & Serving System
서울시 지하철 혼잡도 데이터를 활용한 자동화드 수집(ETL) 파이프라인 및 REST API 서빙 서비스데이터 엔지니어링의 핵심인 오케스트레이션(Airflow), 데이터 웨어하우스(PostgreSQL), 컨테이너 기반 인프라(Docker)를 활용해 데이터 수집부터 적재, 서빙까지의 End-to-End 파이프라인을 구축하는 프로젝트입니다.

📌 1. 프로젝트 개요 (Overview)개발 기간: 2026.07 ~ (진행 중)

개발 목표: 공공 데이터를 주기적으로 안전하게 수집하고, 관계형 데이터베이스에 효율적으로 정규화하여 적재하는 ETL 자동화 파이프라인 구축적재된 데이터를 활용하여 클라이언트의 요청에 빠르게 응답하는 REST API 서버 개발모든 서비스를 Docker 컨테이너로 격리하여 확장성과 재현성이 높은 개발/운영 환경 조성

핵심 기술 스택 (Tech Stack):

OS : Linux(Ubuntu)
Language: Python 3.10
Infrastructure: Docker, Docker Compose(멀티 컨테이너 통합 오케스트레이션)
ComposeOrchestration: Apache Airflow( ETL 파이프라인 자동화 스케줄러) 
Database: PostgreSQL (Data Warehouse)
Backend / API: FastAPI

2. 시스템 아키텍처 (Architecture)시스템은 각 역할별로 컨테이너를 분리하여 느슨한 결합(Loosely Coupled) 구조로 설계되었습니다.
| 구성 요소 | 역할 및 설명 | 기술 스택 |
| :--- | :--- | :--- |
| **Orchestration** | 주기적인 데이터 수집 및 ETL 파이프라인 자동화 | Apache Airflow |
| **Data Warehouse** | 수집된 정형/시계열 데이터 저장 및 정규화 관리 | PostgreSQL |
| **Data Serving** | 적재된 데이터를 REST API 형태로 클라이언트에 제공 | FastAPI |
| **Infrastructure** | 모든 서비스를 컨테이너로 격리하여 실행 및 관리 | Docker, Docker Compose |

📁 3. 디렉토리 구조 (Directory Structure)프로젝트의 유지보수성과 확장성을 고려하여 서비스별로 디렉토리를 명확히 분리했습니다.

| 경로 및 파일명 | 설명 |
| :--- | :--- |
| `subway-congestion-pipeline/` | 프로젝트 루트 디렉토리 |
| ├── `docker-compose.yml` | 전체 서비스 통합 오케스트레이션 설정 파일 |
| ├── `README.md` | 프로젝트 문서 |
| ├── `airflow/` | Apache Airflow 관련 설정 및 DAG 디렉토리 |
| │   ├── `Dockerfile` | Airflow 커스텀 이미지 빌드 파일 |
| │   ├── `dags/subway_congestion_dag.py` | 지하철 혼잡도 수집 및 적재 DAG 파이프라인 |
| │   └── `requirements.txt` | Airflow 파이썬 패키지 의존성 |
| ├── `fastapi/` | FastAPI 백엔드 서빙 디렉토리 |
| │   ├── `Dockerfile` | FastAPI 앱 이미지 빌드 파일 |
| │   ├── `main.py` | API 엔드포인트 구현 (데이터 서빙) |
| │   └── `requirements.txt` | FastAPI 패키지 의존성 |
| └── `postgres/` | PostgreSQL DB 초기화 디렉토리 |
|     └── `init/create_tables.sql` | DB 초기 스키마 자동 생성 SQL 스크립트 |

🗄️ 4. 데이터베이스 스키마 설계 (Database Schema)
데이터의 중복을 최소화하고 조회 성능을 높이기 위해 마스터 테이블(Dimension)과 로그/팩트 테이블(Fact) 구조로 정규화하여 설계했습니다.

① 마스터 테이블 (stations)역 고유 정보와 호선 정보를 관리하는 기준 테이블입니다.
| 컬럼 명 | 데이터 타입 | 제약 조건 (Constraints) | 설명 |
| :--- | :--- | :--- | :--- |
| `station_id` | `VARCHAR(20)` | `PRIMARY KEY` | 역 고유 코드 |
| `station_name` | `VARCHAR(50)` | `NOT NULL` | 역 이름 (예: 강남역) |
| `line_number` | `VARCHAR(20)` | `NOT NULL` | 호선 정보 (예: 2호선) |
);

② 팩트/로그 테이블 (congestion_logs)
시간대별, 요일별로 수집되는 대규모 혼잡도 시계열 데이터를 적재하는 테이블입니다.

| 컬럼 명 | 데이터 타입 | 제약 조건 (Constraints) | 설명 |
| :--- | :--- | :--- | :--- |
| `log_id` | `SERIAL` | `PRIMARY KEY` | 로그 고유 ID (Auto Increment) |
| `station_id` | `VARCHAR(20)` | `REFERENCES stations(station_id)` | 역 고유 코드 (FK) |
| `day_type` | `VARCHAR(20)` | `NOT NULL` | 구분 (평일 / 토요일 / 일요일) |
| `up_down_type` | `VARCHAR(20)` | `NOT NULL` | 상행 / 하행 구분 |
| `time_slot` | `VARCHAR(20)` | `NOT NULL` | 30분 단위 시간대 (예: '08:30') |
| `congestion_rate` | `NUMERIC(5, 2)` | `NOT NULL` | 혼잡도 퍼센트(%) |
| `created_at` | `TIMESTAMP` | `DEFAULT CURRENT_TIMESTAMP` | 데이터 적재 타임스탬프 |


⚙️ 5. 파이프라인 및 데이터 플로우 (Pipeline Workflow)

Apache Airflow DAG를 통해 매주/매일(또는 지정된 스케줄에 따라) 자동으로 실행되는 ETL 프로세스입니다.

Extract (추출):서울 열린데이터광장 오픈 API를 통해 특정 주기의 지하철 혼잡도 JSON 데이터를 HTTP GET 요청으로 가져옵니다.

Transform (변환):응답 데이터 내에서 불필요한 필드를 제거하고, 누락된 데이터(Null 값)나 비정상적인 수치를 필터링합니다.DB에 곧바로 INSERT 할 수 있도록 파이썬 딕셔너리 리스트 형태로 구조를 맞춥니다.

Load (적재):PostgreSQL 데이터베이스와 안전하게 세션을 맺은 뒤, 가공된 데이터를 stations와 congestion_logs 테이블에 일괄 적재합니다.


🚀 6. REST API 서빙 엔드포인트 (API Endpoints)FastAPI를 통해 PostgreSQL에 적재된 데이터를 가공하여 클라이언트에게 제공합니다.

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | 서버 및 DB 연결 상태 확인용 헬스체크 |
| `GET` | `/congestion/{station_name}` | 특정 역의 이름으로 요일별/시간대별 혼잡도 데이터 조회 |
| `GET` | `/congestion/top-crowded` | 특정 시간대에 가장 혼잡한 역 상위 N개 목록 조회 |
