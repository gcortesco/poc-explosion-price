package com.riu.crs.poc_price_explosion_updates.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@SuperBuilder
public class InventoryDto implements Serializable {

    private String hotelId;
    private String rateId;
    private String roomId;
    private String channelId;
    private int inventory;
    private String stayDate;
}
