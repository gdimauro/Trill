// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// The extension methods over interface IQStreamable
    /// </summary>
    public static partial class QStreamableStatic
    {
        /// <summary>
        /// Groups elements and applies a result selector to each grouped window.
        /// </summary>
        /// <typeparam name="TSource">Source element type.</typeparam>
        /// <typeparam name="TKey">Grouping key type.</typeparam>
        /// <typeparam name="TElement">Projected element type in group window.</typeparam>
        /// <typeparam name="TResult">Result projection type.</typeparam>
        /// <param name="source">Source stream.</param>
        /// <param name="keySelector">Key selector expression.</param>
        /// <param name="elementSelector">Element selector expression.</param>
        /// <param name="resultSelector">Result selector applied to each group.</param>
        /// <returns>Projected stream of results.</returns>
        public static IQStreamable<TResult> GroupApply<TSource, TKey, TElement, TResult>(
            this IQStreamable<TSource> source,
            Expression<Func<TSource, TKey>> keySelector,
            Expression<Func<TSource, TElement>> elementSelector,
            Expression<Func<TKey, IWindow<TElement>, TResult>> resultSelector)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));
            if (elementSelector == null) throw new ArgumentNullException(nameof(elementSelector));
            if (resultSelector == null) throw new ArgumentNullException(nameof(resultSelector));

            // Use the GroupBy from Unary.cs
            var grouped = source.GroupBy(keySelector, elementSelector);

            // Build projection: g => resultSelector(g.Key, g.Window)
            var gParam = Expression.Parameter(typeof(IGroupedWindow<TKey, TElement>), "g");
            var body = Expression.Invoke(resultSelector,
                Expression.Property(gParam, nameof(IGroupedWindow<TKey, TElement>.Key)),
                Expression.Property(gParam, nameof(IGroupedWindow<TKey, TElement>.Window)));
            var projection = Expression.Lambda<Func<IGroupedWindow<TKey, TElement>, TResult>>(body, gParam);
            
            // Use the Select from Unary.cs
            return grouped.Select(projection);
        }
    }
}