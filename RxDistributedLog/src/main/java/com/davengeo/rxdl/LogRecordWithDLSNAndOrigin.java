package com.davengeo.rxdl;

import com.twitter.distributedlog.LogRecordWithDLSN;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LogRecordWithDLSNAndOrigin {
    LogRecordWithDLSN logRecordWithDLSN;
    String origin;
}
