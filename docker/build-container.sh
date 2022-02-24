#!/usr/bin/env bash

BRANCH=$1

if [ -z "$S3BUCKET" ] && [ -z "$REGISTRY" ]; then
    echo "No S3BUCKET or REGISTRY set, please set ENV S3BUCKET or REGISTRY"
    exit 1
fi

if [ -z "$BRANCH" ]
  then
    echo "No Branch specified, please specify branch"
    echo "Usage ./build.sh <branch>"
    exit 1
fi

if [ -z "$GIT_OAUTH_TOKEN" ]; then
	echo "Need to set GIT_OAUTH_TOKEN"
	exit 1
fi

if [ ${REGISTRY} ]; then
    IMAGE_NAME=${REGISTRY}/bach-api:${BRANCH}
else
    IMAGE_NAME=bach-api:${BRANCH}
fi

docker rmi -f ${IMAGE_NAME}

docker build \
    --build-arg github_oauth_token=${GIT_OAUTH_TOKEN} \
		--build-arg commit=${GIT_COMMIT} \
    -t ${IMAGE_NAME} -f docker/Dockerfile .

if [ -z "$REGISTRY" ]; then
    docker save bach-api:${BRANCH} -o /tmp/bach-api-${BRANCH}.tar
    aws s3 cp /tmp/bach-api-${BRANCH}.tar s3://${S3BUCKET}/${BRANCH}/bach-api-${BRANCH}.tar

else
    docker push ${IMAGE_NAME}
fi
exit 0
