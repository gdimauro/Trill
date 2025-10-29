// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing.Serializer.Serializers
{
    internal sealed class RecordFieldSerializer
    {
        public ObjectSerializerBase Schema { get; }

        private MyFieldInfo MemberInfo { get; }

        public RecordFieldSerializer(ObjectSerializerBase schema, MyFieldInfo info)
        {
            this.Schema = schema;
            this.MemberInfo = info;
        }

        public Expression BuildSerializer(Expression encoder, Expression @object)
        {
            if (encoder == null) throw new ArgumentNullException(nameof(encoder));
            if (@object == null) throw new ArgumentNullException(nameof(@object));

            var member = GetMember(@object);
            if (this.Schema.RuntimeType.GetTypeInfo().IsValueType || this.MemberInfo.isField)
            {
                return this.Schema.BuildSerializer(encoder, member);
            }

            var tmp = Expression.Variable(this.Schema.RuntimeType, Guid.NewGuid().ToString());
            var assignment = Expression.Assign(tmp, member);
            var serialized = this.Schema.BuildSerializer(encoder, tmp);
            return Expression.Block(new[] { tmp }, new[] { assignment, serialized });
        }

        public Expression BuildDeserializer(Expression decoder, Expression @object)
        {
            if (decoder == null) throw new ArgumentNullException(nameof(decoder));
            if (@object == null) throw new ArgumentNullException(nameof(@object));

            // Obtain member expression first so we can inspect its metadata for writability.
            var memberExpr = GetMember(@object) as MemberExpression;
            bool skipAssignment = false;
            if (memberExpr != null)
            {
                if (memberExpr.Member is PropertyInfo pi)
                {
                    // Skip if property has no setter or setter is non-public.
                    var setter = pi.SetMethod;
                    if (setter == null || !setter.IsPublic) skipAssignment = true;
                }
                // Fields are always writable (given we filtered out readonly scenario in design),
                // but if ever a InitOnly field appears, mark to skip.
                if (memberExpr.Member is FieldInfo fi && fi.IsInitOnly) skipAssignment = true;
            }
            else
            {
                // If we cannot treat it as a MemberExpression (unlikely), be conservative.
                skipAssignment = true;
            }

            if (skipAssignment)
            {
                // Consume value to advance decoder but discard result.
                var discardValue = this.Schema.BuildDeserializer(decoder);
                return Expression.Block(discardValue); // discard
            }

            var value = this.Schema.BuildDeserializer(decoder);
            // Reuse previously created member expression
            var member = memberExpr ?? GetMember(@object);

            if (@object.Type.GetTypeInfo().IsValueType)
            {
                // For value types we need a temp so we can assign then persist.
                var tmp = Expression.Variable(value.Type, "tmpVal");
                return Expression.Block(
                    new[] { tmp },
                    Expression.Assign(tmp, value),
                    Expression.Assign(member, tmp));
            }

            return Expression.Assign(member, value);
        }

        private Expression GetMember(Expression @object)
            => this.MemberInfo.isField
                ? Expression.Field(@object, this.MemberInfo.DeclaringType, this.MemberInfo.OriginalName)
                : Expression.Property(@object, this.MemberInfo.DeclaringType, this.MemberInfo.OriginalName);
    }
}
