package fr.cirad.tools;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.springframework.security.core.Authentication;

import fr.cirad.tools.security.base.AbstractTokenManager;

public final class ExportSizeLimitHelper {

    private static final BigInteger ONE_BILLION = BigInteger.valueOf(1000000000L);

    private ExportSizeLimitHelper() {
    }

    public static int resolveMaxExportableBillionGenotypes(AppConfig appConfig, Authentication auth) {
        int maxBillionGenotypesInvolved = 1;
        String currentUser = AbstractTokenManager.getUserNameFromAuthentication(auth);
        try {
            maxBillionGenotypesInvolved = Integer.parseInt(appConfig.get("maxExportableBillionGenotypes_" + currentUser));
        }
        catch (Exception ignored1) {
            try {
                maxBillionGenotypesInvolved = Integer.parseInt(appConfig.get("maxExportableBillionGenotypes"));
            }
            catch (Exception ignored2) {
            }
        }
        return maxBillionGenotypesInvolved;
    }

    public static String getExportMatrixSizeLimitErrorMessage(AppConfig appConfig, Authentication auth, BigInteger matrixSize) {
        int maxBillionGenotypesInvolved = resolveMaxExportableBillionGenotypes(appConfig, auth);
        if (maxBillionGenotypesInvolved == 0)
            return "You are not allowed to export any genotyping data.";

        BigInteger maxAllowedSize = ONE_BILLION.multiply(BigInteger.valueOf(maxBillionGenotypesInvolved));
        if (matrixSize.compareTo(maxAllowedSize) >= 0)
            return "You may only export up to " + maxBillionGenotypesInvolved + " billion genotypes. The current selection contains " + new BigDecimal(matrixSize).divide(BigDecimal.valueOf(1000000000L)).setScale(2, BigDecimal.ROUND_HALF_UP) + " billion genotypes.";

        return null;
    }
}
