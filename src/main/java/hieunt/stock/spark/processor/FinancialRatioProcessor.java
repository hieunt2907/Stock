package hieunt.stock.spark.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * FinancialRatioProcessor
 * -----------------------
 * Đọc dữ liệu financial_ratio từ MinIO (JSON), transform và ghi vào
 * ClickHouse bảng stock.fact_financial_ratio.
 *
 * Source (MinIO):
 *   s3a://<BUCKET>/financial_ratio/<partitionPath>/<ticker>.json
 *
 * Target (ClickHouse):
 *   stock.fact_financial_ratio
 *
 * Business Key (dedup):
 *   (ticker, year, quarter)
 *
 * Các cột tài chính chuẩn hoá:
 *   pe, pb, ps, roe, roa, eps, ev_ebitda, current_ratio, debt_to_equity,
 *   net_profit_margin, gross_profit_margin, asset_turnover → Decimal(20,4)
 *   year  → Int32
 *   quarter → Int32   ("Q1" / "Yearly" → null-safe)
 */
public class FinancialRatioProcessor extends AbstractMinioToClickhouseProcessor {

    public FinancialRatioProcessor(String partitionPath) {
        super("financial_ratio", "stock.fact_financial_ratio", "financial_ratio", partitionPath);
    }

    /** Dedup theo (ticker, year, quarter) — không dùng cột 'time'. */
    @Override
    protected String[] getBusinessKeys() {
        return new String[]{"ticker", "year", "quarter"};
    }

    @Override
    protected Dataset<Row> transform(Dataset<Row> df) {
        Dataset<Row> result = df;

        // ── Cột phân kỳ (period info) ──────────────────────────────────────
        // financial_ratio() trả về cột 'year' (int hoặc string) và 'quarter'
        // (string: "Q1", "Q2", "Q3", "Q4", "Yearly") — cast về Int32/String.
        if (containsColumn(df, "year")) {
            result = result.withColumn("year",
                    functions.col("year").cast(DataTypes.IntegerType));
        }
        if (containsColumn(df, "quarter")) {
            // Giữ dạng String (Q1..Q4, Yearly)
            result = result.withColumn("quarter",
                    functions.col("quarter").cast(DataTypes.StringType));
        }

        // ── Chỉ số định giá ────────────────────────────────────────────────
        result = castDecimal(result, "pe");
        result = castDecimal(result, "pb");
        result = castDecimal(result, "ps");
        result = castDecimal(result, "ev_ebitda");

        // ── Khả năng sinh lời ─────────────────────────────────────────────
        result = castDecimal(result, "roe");
        result = castDecimal(result, "roa");
        result = castDecimal(result, "eps");
        result = castDecimal(result, "net_profit_margin");
        result = castDecimal(result, "gross_profit_margin");

        // ── Hiệu quả hoạt động ────────────────────────────────────────────
        result = castDecimal(result, "asset_turnover");

        // ── Tính thanh khoản / đòn bẩy ───────────────────────────────────
        result = castDecimal(result, "current_ratio");
        result = castDecimal(result, "debt_to_equity");

        return result;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private boolean containsColumn(Dataset<Row> df, String colName) {
        for (String c : df.columns()) {
            if (c.equalsIgnoreCase(colName)) return true;
        }
        return false;
    }

    /** Cast cột về Decimal(20,4) nếu cột tồn tại, bỏ qua nếu không có. */
    private Dataset<Row> castDecimal(Dataset<Row> df, String colName) {
        if (!containsColumn(df, colName)) return df;
        return df.withColumn(colName,
                functions.col(colName).cast(DataTypes.createDecimalType(20, 4)));
    }
}
