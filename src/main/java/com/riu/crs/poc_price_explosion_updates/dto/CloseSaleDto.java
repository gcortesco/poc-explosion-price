package com.riu.crs.poc_price_explosion_updates.dto;

import lombok.*;
import lombok.experimental.SuperBuilder;

@Getter
@Setter
@Builder
@AllArgsConstructor
public class CloseSaleDto {

    private long hotelId;
    private String rateId;
    private String channelId;
    private boolean closingSale;
}
