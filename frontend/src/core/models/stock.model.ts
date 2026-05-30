export interface StockResponse {
    symbol: string;
    companyName: string;
    exchange: string;
    industry: string;
    sector: string;
    tradingDate: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    value: number;
    dailyReturn: number;
    volatility: number;
    source: string;
}

export interface StockTickResponse {
    eventId: string;
    symbol: string;
    eventTime: string;
    price: number;
    volume: number;
    source: string;
}

export interface StockOhlcResponse {
    symbol: string;
    windowStart: string;
    windowEnd: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    value: number;
    source: string;
}
