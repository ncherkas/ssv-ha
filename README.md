# ssv-ha

## Run instructions
```
    go build 
    ./duty-processor -endpoint=ws://127.0.0.1:5000
```

## Notes & Considerations
 1) I've implemented this with a focus on possible re-orderings originating from WS stream. The idea behind my solution is to ensure that no duties are performed out of order. For this I had to relax data loss - if re-ordered late duties come too late they are discarded. 
 The wait time is configured and same way we could configure whethe the late duties are discarded or put into some deadletter queue.
 2) Data model is a bit simplified so that the WS client types are used by the processor. Ideally I would introduce yet another type dedicated for the processor which would accept data from `ws.DutyMsg` and extend it with `ReceivedAt`.
 3) Sorry didn't have enough time to cover everything nicely with unit tests so I rather left a end-to-end sample which can be extended with different combinations to cover possible scenarios.
 4) Also, if I had more time I'd add docker-compose running both `mock_duties` and `ss-ha` (the processor).
 5) Among other improvements - use of config lib (viper or smth similar) and some popular logger.