docker build -t <your-registry>/pod-api-requester:<tag> --target debug .
# or
docker build -t <your-registry>/pod-api-requester:<tag> --target production .
# The default build is production
docker build -t <your-registry>/pod-api-requester:<tag>


docker push <your-registry>/pod-api-requester:<tag>