./gradlew uber
export TF_VAR_image_tag=$(git rev-parse --short HEAD)
cp  app/build/libs/app-uber-*.jar app/infra/image/app-uber.jar
docker build --platform linux/amd64 -t postevent/app:$TF_VAR_image_tag app/infra/image/
docker tag postevent/app:$TF_VAR_image_tag 937578967415.dkr.ecr.eu-west-1.amazonaws.com/p14n/postevent-app:$TF_VAR_image_tag
aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 937578967415.dkr.ecr.eu-west-1.amazonaws.com
docker push 937578967415.dkr.ecr.eu-west-1.amazonaws.com/p14n/postevent-app:$TF_VAR_image_tag

