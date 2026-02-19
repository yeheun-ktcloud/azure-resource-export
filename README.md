# 사전 환경 세팅
### 1. 스크립트 다운로드 (최신 버전 사용)

```
git clone https://github.com/yeheun-ktcloud/azure-resource-export.git
```

### 2. Python 최신 버전 설치
- 공식 사이트 : Download Python (https://www.python.org/downloads/)
- Windows: Python Releases for Windows - Stable Releases (https://www.python.org/downloads/windows/)
- MacOS: Python Releases for macOS - Stable Releases (https://www.python.org/downloads/macos/)
- 설치 시 “Add python.exe to PATH” 옵션 체크
- 설치 완료 시 확인 명령어
```
python --version
```
### 3. 스크립트 실행을 위한 Python 모듈 설치
- requirements.txt 파일이 존재하는 경로에서 실행 (requirements.txt 파일이 암호화된 경우 암호화를 해제하고 실행해주세요!!)
```
pip install -r requirements.txt
```

### 4. az cli login
- KT 테넌트에 로그인
```
az login --tenant e6c9ec09-8430-4a99-bf15-242bc089b409
```

#  스크립트 실행
### 1. main.py 실행
- main.py 파일이 존재하는 경로에서 실행
```
python main.py
```
### 2. 각 질문 항목에 대해 입력
```
DEV Subscription ID? : (개발 구독 ID)
STG Subscription ID? : (스테이징 구독 ID)
PRD Subscription ID? : (운영 구독 ID)
DEV Resource Group Name? (개발 리소스그룹 이름)
STG Resource Group Name? (스테이징 리소스그룹 이름)
PRD Resource Group Name? (운영 리소스그룹 이름)
* 리소스그룹이 복수 개인 경우 , 사용하여 입력
단위서비스코드? : (단위서비스코드)
단위서비스 약어? : (단위서비스 약어)
출력 폴더 경로? (없으면 엔터=현재 폴더): (파일 저장되는 경로)
```

### 3. 배포되어 있는 리소스 타입 스캔 후 관련 정보 조회, 조회 완료되면 Excel 파일 생성
</br>

## ※ 자세한 내용은 https://ktcloud.atlassian.net/wiki/x/LgNSZ 참고 부탁드립니다. ※
