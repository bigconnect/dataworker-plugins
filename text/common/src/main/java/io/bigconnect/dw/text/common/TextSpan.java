package io.bigconnect.dw.text.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TextSpan {
    int start;
    int end;
    String text;
}
