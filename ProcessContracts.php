<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Http\Request;
use App\ShipoContractTransaction;
use App\Configuration;

class ProcessContracts extends Controller
{
    public function __invoke()
    {
        return $this->processPivot();
    }

    public function processPivot()
    {
        $processName = 'pivot_process';

        try {

            if (!Configuration::lockProcess($processName)) {
                Log::channel('clvgen')->error("El procesamiento de pivot de contratos ya estaba en curso.");
                return 0;
            }

            Log::channel('clvgen')->info('Procesando pivot de contratos...');

            $processedPivots = 0;
            $currentTransaction = null;
            $contractBeneficiaries = [];
            $currentTransactionKey = '';
            $transactionIndex = 0;

            // traemos los contratos nuevos y renovaciones de la tabla de pivot
            DB::connection('informes_analyzer')->table('shipo_contract_transactions_pivot')
                ->orderBy('subscription_id')
                ->orderBy('contract_id')
                ->orderBy('transaction_date')
                ->orderBy('transaction_type', 'desc')
                ->chunk(1000, function ($pivots) use (
                    &$processName,
                    &$processedPivots,
                    &$currentTransaction,
                    &$contractBeneficiaries,
                    &$currentTransactionKey,
                    &$transactionIndex
                ) {
                    foreach ($pivots as $pivot) {
                        if ($pivot->transaction_type == 'baja') {
                            ShipoContractTransaction::cancelContract($pivot);
                        } else { //nuevos y renovaciones
                            if (($pivot->contract_id . $pivot->subscription_id . $pivot->transaction_type . $pivot->transaction_date) != $currentTransactionKey) {
                                // insertamos en la base de datos la transacción anterior
                                if (isset($currentTransaction)) {
                                    ShipoContractTransaction::createFromPivot($currentTransaction, $contractBeneficiaries)->store();
                                }

                                // empezamos a formar la nueva
                                $currentTransaction = $pivot;
                                $contractBeneficiaries = [];
                                $currentTransaction->gets_credential = 0;
                                $currentTransactionKey = $pivot->contract_id . $pivot->subscription_id . $pivot->transaction_type . $pivot->transaction_date;
                            } else {
                                $currentTransaction->days = $currentTransaction->days . $pivot->days;
                                if (stripos($currentTransaction->rate, trim($pivot->rate)) === FALSE) {
                                    $currentTransaction->rate = $currentTransaction->rate . trim($pivot->rate) . ',';
                                }
                            }

                            //producto principal
                            if ($pivot->main_product = 'S') {
                                $currentTransaction->product = $pivot->product;
                                $currentTransaction->edition = $pivot->edition;
                            }

                            // tratamos por separado los beneficiarios porque pueden ser varios por contrato
                            if (!empty($pivot->beneficiary_cst)) {
                                array_push($contractBeneficiaries, $pivot->beneficiary_cst);
                            }

                            if ($pivot->rate == 'CREDENCIAL CLV' || $pivot->rate == 'CREDENCIAL ULTIMO MARTES'
                                    || $pivot->rate == 'CREDENCIAL ULTIMO DOMINGO') {
                                $currentTransaction->gets_credential = 1;
                            }
                        }
                        $processedPivots++;
                    }

                });

            // insertamos la última transaccion
            if ($currentTransaction !== null) {
                ShipoContractTransaction::createFromPivot($currentTransaction, $contractBeneficiaries)->store();
            }

            // truncamos la tabla de pivot
            DB::connection('informes_analyzer')->table('shipo_contract_transactions_pivot')->truncate();

            Log::channel('clvgen')->info("Se procesaron $processedPivots registros");

            Configuration::unlockProcess($processName);

        } catch (\Exception $e) {
            Log::channel('clvgen')->error("Hubo problemas para procesar el pivot de contratos.");
            Log::channel('clvgen')->error($e->getMessage());
        } finally {
            Configuration::unlockProcess($processName);
        }

    }
}
