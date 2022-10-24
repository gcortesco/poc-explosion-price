package com.riu.crs.poc_price_explosion_updates.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@Getter
@Setter
@AllArgsConstructor
@Builder
public class NewContractPriceDto extends PriceDto {

    private long contractId;
}
