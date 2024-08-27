/*
 * Copyright 2021 The DAPHNE Consortium
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_RUNTIME_LOCAL_KERNELS_MAP_H
#define SRC_RUNTIME_LOCAL_KERNELS_MAP_H

#include <runtime/local/context/DaphneContext.h>
#include <runtime/local/datastructures/DataObjectFactory.h>
#include <runtime/local/datastructures/DenseMatrix.h>
#include <runtime/local/datastructures/Matrix.h>

#include <algorithm>

// ****************************************************************************
// Struct for partial template specialization
// ****************************************************************************

template<class DTRes, class DTArg>
struct Map {
    // We could have a more specialized function pointer here i.e. (DTRes::VT)(*func)(DTArg::VT).
    // The problem is that this is currently not supported by kernels.json.
    static void apply(DTRes *& res, const DTArg * arg , void* func, DCTX(ctx)) = delete;
};

// ****************************************************************************
// Convenience function
// ****************************************************************************

template<class DTRes, class DTArg>
void map(DTRes *& res, const DTArg * arg, void* func, DCTX(ctx)) {
    Map<DTRes, DTArg>::apply(res, arg, func, ctx);
}

// ****************************************************************************
// (Partial) template specializations for different data/value types
// ****************************************************************************

// ----------------------------------------------------------------------------
// DenseMatrix
// ----------------------------------------------------------------------------

template<typename VTRes, typename VTArg>
struct Map<DenseMatrix<VTRes>, DenseMatrix<VTArg>> {
    static void apply(DenseMatrix<VTRes> *& res, const DenseMatrix<VTArg> * arg, void* func, DCTX(ctx)) {
        const size_t numRows = arg->getNumRows();
        const size_t numCols = arg->getNumCols();
        
        if (res == nullptr)
            res = DataObjectFactory::create<DenseMatrix<VTRes>>(numRows, numCols, false);
        
        auto udf = reinterpret_cast<VTRes(*)(VTArg)>(func);

        const VTArg * valuesArg = arg->getValues();
        VTRes * valuesRes = res->getValues();

        for(size_t r = 0; r < numRows; r++) {
            for(size_t c = 0; c < numCols; c++)
                valuesRes[c] = udf(valuesArg[c]);
            valuesArg += arg->getRowSkip();
            valuesRes += res->getRowSkip();
        }
    }
};

// ----------------------------------------------------------------------------
// Matrix
// ----------------------------------------------------------------------------

// template<typename VTRes, typename VTArg>
// struct Map<Matrix<VTRes>, Matrix<VTArg>> {
//     static void apply(Matrix<VTRes> *& res, const Matrix<VTArg> * arg, void* func, DCTX(ctx)) {
//         const size_t numRows = arg->getNumRows();
//         const size_t numCols = arg->getNumCols();
        
//         if (res == nullptr)
//             res = DataObjectFactory::create<DenseMatrix<VTRes>>(numRows, numCols, false);
        
//         auto udf = reinterpret_cast<VTRes(*)(VTArg)>(func);

//         res->prepareAppend();
//         for (size_t r = 0; r < numRows; ++r)
//             for (size_t c = 0; c < numCols; ++c)
//                 res->append(r, c, udf(arg->get(r, c)));
//         res->finishAppend();
//     }
// };

// #endif //SRC_RUNTIME_LOCAL_KERNELS_MAP_H



// Updated Map implementation to support specific row/column application
template<typename VTRes, typename VTArg>
struct Map<DenseMatrix<VTRes>, DenseMatrix<VTArg>> {
    static void apply(DenseMatrix<VTRes> *& res, const DenseMatrix<VTArg> * arg, void* func, bool isMatrix, bool isRow, int64_t index, DCTX(ctx)) {
        const size_t numRows = arg->getNumRows();
        const size_t numCols = arg->getNumCols();

        if (isMatrix) {
            if (res == nullptr)
                res = DataObjectFactory::create<DenseMatrix<VTRes>>(numRows, numCols, false);

            auto udf = reinterpret_cast<VTRes(*)(VTArg)>(func);

            if (isRow) {
                // Apply the UDF to the specific row
                if (index < numRows) {
                    VTRes * rowValuesRes = res->getValues() + index * res->getRowSkip();
                    const VTArg * rowValuesArg = arg->getValues() + index * arg->getRowSkip();
                    for (size_t c = 0; c < numCols; ++c)
                        rowValuesRes[c] = udf(rowValuesArg[c]);
                }
                else {
                    // Handle the error of invalid row index appropriately
                }
            } else {
                // Apply the UDF to the specific column
                if (index < numCols) {
                    for (size_t r = 0; r < numRows; ++r) {
                        VTRes * valuesRes = res->getValues() + r * res->getRowSkip() + index;
                        const VTArg * valuesArg = arg->getValues() + r * arg->getRowSkip() + index;
                        *valuesRes = udf(*valuesArg);
                    }
                }
                else {
                    // Handle the error of invalid column index appropriately
                }
            }
        } else {
            // Scalar case: Apply the function and sum up the results or reduce in some way.
            auto udf = reinterpret_cast<VTRes(*)(VTArg)>(func);
            VTRes scalarResult = VTRes(); // Assuming a default initialization

            if (isRow) {
                // Reduce over specific row
                if (index < numRows) {
                    for (size_t c = 0; c < numCols; ++c) {
                        scalarResult += udf(arg->get(index, c));
                    }
                }
                else {
                    // Handle the error of invalid row index appropriately
                }
            } else {
                // Reduce over specific column
                if (index < numCols) {
                    for (size_t r = 0; r < numRows; ++r) {
                        scalarResult += udf(arg->get(r, index));
                    }
                }
                else {
                    // Handle the error of invalid column index appropriately
                }
            }

            res = DataObjectFactory::create<DenseMatrix<VTRes>>(1, 1, false);
            res->set(0, 0, scalarResult);
        }
    }
};

#endif //SRC_RUNTIME_LOCAL_KERNELS_MAP_H
