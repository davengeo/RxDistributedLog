package com.davengeo.rxdl;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.ByteBuffer;

@SuppressWarnings("WeakerAccess")
@Data
@AllArgsConstructor
public class EmissionWithOrigin {

    ByteBuffer payload;
    String origin;

}
