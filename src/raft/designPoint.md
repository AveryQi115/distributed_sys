# 设计要点+踩坑记录

## SendHeartBeat

- 向其他server发出的应该是并发异步的！！！因为server可能不会响应，如果设置成顺序并等待结果，就会一直卡在发出请求的线程里
- 论文要求：如果过半server没有响应yes，leader应该转follower；实际没有实现，因为如果leader后续收到term更高的请求，也能转follower

## RequestVote

- 向其他server发出的应该是并发异步的！！！因为server可能不会响应，如果设置成顺序并等待结果，就会一直卡在发出请求的线程里
- 收到RequestVote请求的server只有在grant vote时才会重置election timer，如果返回false，不重置
- 上述那条很重要：如果收到Request Vote就重置timer，可能导致candidate因为log过期永远无法成为leader，而follower因为重复收到Request请求，不触发election timeout
- 收到RequestVote请求的server如果收到term更高的请求，转follower并更新term，但是还是需要通过后续检查才能vote！

## AppendEntries

- 如果收到leader请求term小于当前server term，代表该请求不是当前leader发出的，忽略并返回自己的term（忽略就是说不重置timer）
- 如果收到leader请求term大于或等于当前server term，立刻更新当前server term并重置timer
- 先match prevLogIndex，再一一比对自己log entries后续的部分和请求中后续的entries
- 如果比对一直成功到比对完请求中的新entries，自己的log entries后续所有部分保留（包括长于请求中的新entries的部分）
- 如果比对一直成功直到比对完当前entries，entries继续添加后续的新entries

## Leader log committed

- leader维护nextIndex和matchIndex，发起appendEntries请求时用nextIndex-1传入prevLogIndex，entries传入[nextIndex:)，如果返回yes，更新matchIndex为prevLogIndex，nextIndex还可以增加的情况下增加nextIndex

- nextIndex可以设计为增加至nextIndex+len(entries),减少rpc调用次数（目前设计是increment 1）

- 如果返回no，nextIndex-1再次发起AppendEntries请求直到matched

- leader另设线程持续性检查matchIndex数组，在其[当前commitIndex，中位数]中选择最大的对应term等于当前term的log index，标记为committed，返回ApplyMsg

- 下一次心跳或者AppendEntries请求，其他server根据当前commitIndex更新他们自己的commitIndex

  