package com.hieunt.stock.repository.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "watchlists", uniqueConstraints = {
        @UniqueConstraint(name = "uk_watchlists_created_by_symbol", columnNames = {"created_by", "symbol"})
})
@Getter
@Setter
@NoArgsConstructor
public class WatchlistEntity extends BaseEntity {

    @Column(name = "symbol", nullable = false, length = 20)
    private String symbol;

    public WatchlistEntity(String symbol) {
        this.symbol = symbol;
    }
}
