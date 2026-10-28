package com.example.orderproducer.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

/**
 * The JSON body a client sends to {@code POST /api/orders}.
 *
 * <p>This is an <b>API contract</b> (what HTTP clients send), deliberately separate from
 * {@link com.example.orderproducer.event.OrderEvent} (what goes onto Kafka). Keeping them separate
 * means we can change the REST API without breaking consumers, and vice versa.
 *
 * <p>The validation annotations are checked when the controller parameter is marked {@code @Valid}.
 * {@code BigDecimal} is used for money because {@code double} cannot represent values like
 * 0.1 exactly and produces rounding errors.
 */
public record OrderRequest(

        @NotBlank(message = "orderId must not be blank")
        String orderId,

        @NotBlank(message = "productName must not be blank")
        String productName,

        @NotNull(message = "quantity is required")
        @Positive(message = "quantity must be greater than zero")
        Integer quantity,

        @NotNull(message = "price is required")
        @Positive(message = "price must be greater than zero")
        BigDecimal price
) {
}
