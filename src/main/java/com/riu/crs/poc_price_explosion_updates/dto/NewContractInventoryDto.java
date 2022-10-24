package com.riu.crs.poc_price_explosion_updates.dto;

import lombok.*;
import lombok.experimental.SuperBuilder;

@Getter
@Setter
@SuperBuilder
public class NewContractInventoryDto extends InventoryDto {

    private long contractId;
}
