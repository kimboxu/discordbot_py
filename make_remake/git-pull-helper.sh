#!/bin/bash

# 설정 - 원하는 기본 브랜치 (보통 main 또는 master)
DEFAULT_BRANCH="main"
DEFAULT_REMOTE="origin"

# 현재 저장소 상태 확인
current_branch=$(git symbolic-ref --short HEAD 2>/dev/null)
if [ $? -ne 0 ]; then
    echo "현재 브랜치에 있지 않습니다. $DEFAULT_BRANCH 브랜치로 전환합니다..."
    
    # 변경사항이 있는지 확인
    if [ -n "$(git status --porcelain)" ]; then
        echo "로컬 변경사항이 있습니다. 변경사항을 버리고 계속할까요? (y/n)"
        read answer
        if [ "$answer" = "y" ]; then
            # 변경사항 버리기
            git reset --hard
        else
            echo "작업을 취소합니다."
            exit 1
        fi
    fi
    
    # 브랜치 목록 확인
    git fetch $DEFAULT_REMOTE
    branch_exists=$(git branch -a | grep -w "$DEFAULT_BRANCH" | wc -l)
    remote_branch_exists=$(git branch -a | grep -w "remotes/$DEFAULT_REMOTE/$DEFAULT_BRANCH" | wc -l)
    
    if [ $branch_exists -gt 0 ]; then
        # 로컬 브랜치가 존재하면 해당 브랜치로 전환
        git checkout $DEFAULT_BRANCH
    elif [ $remote_branch_exists -gt 0 ]; then
        # 원격 브랜치만 존재하면 해당 브랜치를 추적하는 로컬 브랜치 생성
        git checkout -b $DEFAULT_BRANCH $DEFAULT_REMOTE/$DEFAULT_BRANCH
    else
        # 만약 지정한 기본 브랜치가 없다면 사용자에게 브랜치 선택 요청
        echo "지정한 기본 브랜치($DEFAULT_BRANCH)를 찾을 수 없습니다."
        echo "사용 가능한 브랜치 목록:"
        git branch -a
        echo "브랜치 이름을 입력하세요:"
        read branch_name
        git checkout $branch_name || git checkout -b $branch_name $DEFAULT_REMOTE/$branch_name
    fi
fi

# 현재 브랜치에서 pull 실행
current_branch=$(git symbolic-ref --short HEAD)
echo "브랜치 '$current_branch'에서 pull 실행 중..."
git pull

echo "완료!"