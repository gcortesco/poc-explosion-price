package com.riu.crs.poc_price_explosion_updates.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
public class PriceDto implements Serializable {
    private String hotelId;
    private String rateId;
    private String roomId;
    private String boardId;
    private String channelId;
    private String fromDay;
    private String toDay;
    private PriceDetail priceDetail;

    @Getter
    @Setter
    @NoArgsConstructor
    public static class PriceDetail implements Serializable {
        private int totalNet;
        private int totalTax;
        private int totalGross;
        private int totalVax;
        private String currency;
    }
}
