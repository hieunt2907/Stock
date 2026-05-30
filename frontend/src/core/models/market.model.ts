export interface MarketSummaryResponse {
    tradingDate: string;
    totalSymbols: number;
    totalVolume: number;
    totalValue: number;
    avgReturn: number;
    advancers: number;
    decliners: number;
    unchanged: number;
    ingestionTime: string;
}

export interface MarketTopGainerResponse {
    tradingDate: string;
    symbol: string;
    companyName: string;
    sector: string;
    close: number;
    dailyReturn: number;
    rank: number;
    ingestionTime: string;
}

export interface MarketTopLiquidityResponse {
    tradingDate: string;
    symbol: string;
    companyName: string;
    sector: string;
    volume: number;
    value: number;
    rank: number;
    ingestionTime: string;
}

export interface MarketSectorResponse {
    tradingDate: string;
    sector: string;
    totalSymbols: number;
    totalVolume: number;
    totalValue: number;
    avgReturn: number;
    bestSymbol: string;
    worstSymbol: string;
    ingestionTime: string;
}
