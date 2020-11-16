1. 실행방법
---
sudo make

sudo make install


(필요한 라이브러리)

libdev-fuse

redis-server


2. 확인된 문제들
---
만약 ```touch a.txt```를 실행한다면, getattr -> create -> getattr -> utimens -> getattr 순으로 시스템콜이 진행되는 것 같은데, 각 시스템콜 사이에서 reply가 "가끔" 공유되는 현상이 발생함.(dequeue하기 전에는 req->reply가 NULL이어야하는데 그 전의 시스템콜에서 호출한 reply가 남아있음-함수 끝부분에 req=NULL을 해주더라도!- 심지어 그 전의 시스템콜에서 호출한 reply는 redis에 들어가지 않음)  왜 그런지는 모르겠음. sleep으로 delay를 주거나, create와 utimens에 cond wait을 줘버리면 잘 돌아감. lock 관련 문제인 것으로 보임.

때때로 queue에서 dequeue될때, command가 깨져서 나오는 현상이 발생함. (keys * -> keys%&#^ 이런식) 대체로 이 현상이 발생하고나서 위에서 언급한 문제가 발생함.
